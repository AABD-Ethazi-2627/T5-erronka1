# CNC Guard — Erronkako galderen erantzunak (G1–G12)

"Pentsatzeko galderak" ez dira zuzenean puntuatzen, baina gure erabakiak azaltzen dituzte. Erantzunetako zenbakiak gure sistemaren eta notebook-aren benetako emaitzak dira.

---

## 1. fasea

**G1. Zer gertatuko litzateke Filebeat registry-rik gabe berrabiaraziko bagenu? Eta NiFi-k fitxategiak zuzenean irakurriko balitu?**

Registry-ak (`filebeat-data` bolumena) fitxategi bakoitza zein lerrotaraino irakurri den gordetzen du. Hura gabe, Filebeat hutsetik hasiko litzateke eta lerro guztiak berriz bidaliko lituzke: **datu bikoiztuak** Kafka-n eta Elasticsearch-en.

Praktikan egiaztatu genuen: zerbitzariaren IPa aldatzean Filebeat berriz sortu genuen; 303 fitxategi aurkitu zituen eta 93 berriak bakarrik bidali zituen (930 lerro), registry-a gorde zelako.

NiFi-k fitxategiak zuzenean irakurriko balitu, berak eraman beharko luke kontrol hori. `ListFile`-k azken fitxategiaren data gordetzen du, baina ez daki hazten ari den fitxategi baten lerro berriak bakarrik irakurtzen. `GetFile`-k, gainera, fitxategia ezabatu egiten du irakurri ondoren.

**G2. Zergatik da karpeta irakurtzeko soilik? Zer gertatuko litzateke idazketa-baimenarekin eta `GetFile`-rekin?**

Gelako talde guztiek partekatzen dute karpeta. `GetFile`-k (`Keep Source File = false`, lehenetsia) fitxategiak ezabatu egiten ditu irakurri ondoren: aktibatzen duen lehen ikasleak datu gabe utziko lituzke gainerako guztiak. Irakurtzeko soilik muntatuta (`ro` aukera eta zerbitzariaren baimenak), inork ezin ditu jatorrizko datuak ezabatu edo aldatu.

**G3. Zer gertatuko litzateke `exclude_lines` gabe goiburua Elasticsearch-era iritsiko balitz?**

Goiburua (`timestamp,machine_id,...,rpm,...`) beste irakurketa bat bezala tratatuko litzateke. Gure indizeetan `rpm` `integer` da eta `timestamp` `date`; "rpm" edo "timestamp" testua ezin da bihurtu, beraz **mapping-errorea** emango luke eta dokumentua baztertuko litzateke. Mapping-ik gabe, Elasticsearch-ek eremuak testu gisa sor litzake, eta Kibanako grafikoak eta batezbestekoak hondatuko lirateke. Gainera, 30 segundoan behin zabor-lerro bat iritsiko litzateke (fitxategi bakoitzeko bat).

Gure sisteman beste bi babes daude: NiFi-ko CSVReader-ek eskema bat erabiltzen du (zenbaki ez den `rpm` batek errorea ematen du), eta API-ak `machine_type` L/M/H dela egiaztatzen du. Elasticsearch-en, `cnc-sensors-pipeline`-k `machine_id = "machine_id"` duten lerroak baztertzen ditu.

**G4. Zein izango lirateke zuzendarientzako hiru KPI garrantzitsuenak? Zergatik?**

1. **Hutsegite-tasa** (hutsegiteak / irakurketak, ~% 3): fabrikaren egoera orokorra.
2. **Orain arriskuan dauden makinak** (KONTUZ edo LARRIA): gaur erantzun behar zaiona.
3. **Saihestutako kostua** (garaiz detektatutako hutsegiteak × 4.800 €): sistema dirutan adierazten du, zuzendaritzak ulertzen duen hizkuntzan. Ekoizpenean, 235 hutsegite garaiz detektatu ziren: ~1,1 milioi € saihestuta, alerta faltsuen kostua kenduta.

Dashboard-ean, erronkak eskatzen duen bezala, **makina aktiboak** ere badaude: datuak iristen ari diren erakusten du (sistemak funtzionatzen duen).

---

## 2. fasea

**G5. Zer gertatzen da accuracy-rekin bakarrik ebaluatzen badugu? Zer puntuazio izango luke beti "0" esaten duen eredu batek?**

Datuen % 3,39 bakarrik dira hutsegiteak (8.000tik 271). Beti "ez du huts egingo" esaten duen eredu batek **% 96,6ko accuracy-a** lortuko luke, **hutsegite bakar bat ere detektatu gabe** (recall = 0). Larria da: eredu bikaina dirudi eta ez du ezertarako balio, detektatu nahi duguna hutsegiteak direlako. Horregatik erabiltzen ditugu recall, precision, F1, ROC-AUC, confusion matrix-a eta kostua.

**G6. Recall/precision trade-off-a: zer esan nahi du ARAKAIN-entzat? Non dago oreka?**

Recall-a igotzen badugu (hutsegite gehiago detektatu), precision-a jaisten da (alerta faltsu gehiago). ARAKAIN-entzat:

- Detektatu gabeko hutsegite bat (**FN**) = makina geldirik, 1.200 €/h × 4 h = **4.800 €**.
- Alerta faltsu bat (**FP**) = beharrezkoa ez den ikuskapena, **150 €** suposatu dugu.

FN bat FP bat baino 32 aldiz garestiagoa da; beraz, oreka **recall altuan** dago, alerta faltsu batzuk onartuz. Baina ez gehiegi: alerta ia guztiak faltsuak badira, teknikariek ez diete kasurik egingo.

Atalaseak `FN × 4.800 + FP × 150` kostuarekin eta precision-recall kurbarekin alderatu ditugu (validation: 1.600 irakurketa, 54 hutsegite):

| Atalasea | Recall | Precision | FN | FP | Kostua |
|---|---|---|---|---|---|
| 0,50 | 0,76 | 0,89 | 13 | 5 | 63.150 € |
| **0,15** | **0,89** | **0,41** | **6** | **69** | **39.150 €** |

**0,15** aukeratu dugu: 13 hutsegite galdu beharrean 6 galtzen dira, eta kostua % 38 jaisten da. 0,11 atalasea apur bat merkeagoa litzateke (34.350 €), baina alerta faltsu gehiagorekin. API-ak atalase bera erabiltzen du (KONTUZ ≥ 0,15). Ekoizpenean oreka berdina da: recall 0,91, precision 0,40.

**G7. Zein da aldagai garrantzitsuena (`feature_importances_`)? Intuizioarekin bat dator?**

`torque_Nm` 0,22 · `tool_wear_min` 0,20 · `rpm` 0,19 · `power_est` 0,19 · `temp_diff` 0,10 · `air_temp_C` 0,06 · `process_temp_C` 0,04 · `machine_type` 0,01.

Bai, intuizioarekin bat dator. Torque-a eta higadura espero genituen: higatuta dagoen eta esfortzu handia egiten duen tresna bat lehenago apurtzen da (gainkarga, OSF, eta higadura, TWF). Abiadura eta potentzia ere goian daude, potentzia-muturrek ere hutsegiteak eragiten dituztelako (PWF). Guk sortutako `power_est` eta `temp_diff` lehen bostetan daude: feature engineering-ak balio izan du. Makina-motak ia ez du eraginik.

**G8. Zergatik da garrantzitsua data leakage-a saihestea (test datuak entrenatzeko ez erabiltzea)?**

Eredua gero ebaluatuko den datu berberekin entrenatzen bada, "buruz ikasten" ditu eta metrikak puztuta ateratzen dira: dena asmatzen duela dirudi, baina datu berriekin huts egingo luke. Simulagailuak `ai4i2020_test.csv` erabiltzen du, ereduak inoiz ikusi ez dituen datuekin nola funtzionatzen duen egiaztatzeko, benetako fabrikan bezala. Horregatik `ai4i2020_train.csv`-rekin bakarrik entrenatzen dugu, eta horren barruan train/validation zatiketa `stratify=y`-rekin egiten dugu. SMOTE ere entrenamendu-zatian bakarrik aplikatzen da (`ImbPipeline`).

Emaitzak berresten du: test datuekin, ekoizpenean, recall-a 0,91 da (notebook-ean 0,89). Eredua ez da "buruz ikasitakoa".

Arrazoi beragatik ez ditugu `failure_*` zutabeak (TWF, HDF...) erabili: hutsegitea bera dira, eta fabrikan ez daude makinak huts egin aurretik. `machine_failure` API-ra iristen da eta Elasticsearch-en gordetzen da, baina **ez zaio ereduari pasatzen**: ekoizpenean ereduaren asmatze-maila neurtzeko bakarrik da.

---

## 3. fasea

**G9. Zergatik kargatzen du API-ak eredua behin abiaraztean, eta ez eskaera bakoitzean?**

`.pkl` fitxategia diskotik kargatzeak denbora behar du (200 zuhaitzeko Random Forest batek hainbat MB ditu). Eskaera bakoitzean kargatuko balitz, segundoro datuak bidaltzen dituzten 1.000 makinarekin segundoko 1.000 karga izango lirateke: API-a oso motel joango litzateke, eskaerak NiFi-n pilatuko lirateke eta ez litzateke "denbora errealeko iragarpena" izango. `lifespan`-ean behin kargatuta, eredua memorian geratzen da eta iragarpen bakoitzak milisegundo batzuk behar ditu (API-ak `X-prozesatze-denbora` goiburuan neurtzen du).

**G10. Alerta bat pizten denean, zer egin beharko luke enpresak? Kibana nahikoa da? Nola automatiza daiteke?**

Kibanako alerta bat ez da nahikoa: norbaitek pantailari begira egon behar du. Hau egin beharko litzateke:

- **Mantentze-arduradunari aktiboki abisatu** LARRIA pizten denean. **Guk hori egin dugu:** NiFi-ko fluxuan, API-aren erantzunetik `alert_level` ateratzen da (`EvaluateJsonPath`), LARRIA bada (`RouteOnAttribute`) mezua prestatzen da (`ReplaceText`), `ControlRate`-k makina bakoitzeko 2 minutuan mezu bat uzten du (teknikaria mezuz ez gainezteko), eta `InvokeHTTP` batek Telegram-era bidaltzen du: makina, probabilitatea eta ordua. API-ak irakurketa bat baztertzen badu (datu okerra), beste abisu bat bidaltzen da. Doakoa da (Kibanako konektoreek ordainpeko lizentzia behar dute).
- **Lan-agindu bat automatikoki sortu** mantentze-sisteman.
- **Erregistratu** zer egin zen alerta bakoitzarekin (benetakoa ala faltsua), eredua hobetzeko.

Makina gelditzeko erabakia pertsona batek hartzen jarraitzen du (giza gainbegiraketa, AI Act 14. art.).

**G11. Zer abantaila eta desabantaila ditu bi indize izateak (`cnc-sensors` eta `cnc-predictions`)?**

Abantailak:

- Datu gordinak eta ereduaren emaitzak bereizita: eredua aldatzean iragarpenak ezabatu eta berriz sor daitezke, jatorrizko datuak ukitu gabe.
- Indize bakoitzak bere mapping-a eta politika (adibidez, iragarpenak denbora gutxiagoz gordetzea ILM-rekin).

Desabantailak:

- Datuak bikoiztu egiten dira (`machine_id`, `timestamp`, neurketak) eta leku gehiago hartzen dute.
- Bi kontsumitzaile-talde eta bi NiFi fluxu mantendu behar dira.
- Irakurketa bat eta bere iragarpena batera ikusteko bi indizeak gurutzatu behar dira (Elasticsearch-ek ez du JOIN-ik).

**Gure erabakia:** 1. fasean bi indize erabili genituen. 3. fasearen ondoren **fluxu eta indize bakarrera** pasa ginen: `cnc-predictions`-ek irakurketa, iragarpena eta benetako emaitza gordetzen ditu dokumentu berean, eta bi dashboard-ek indize bera erabiltzen dute. Horri esker ekoizpeneko confusion matrix-a kalkula daiteke JOIN-ik gabe. `cnc-sensors` 1. faseko emaitza gisa gorde dugu.

**G12. Eredua duela 6 hilabete entrenatu zen eta fabrikak makina-mota berri bat gehitu du: zer arazo dago eta nola konpontzen da?**

**Model drift**-a da: ereduak L, M eta H makinekin ikasi zuen, eta makina berri batek bestelako portaera izan dezake (beste tenperatura batzuk, beste rpm tarte batzuk). Gainera, gure API-ak `machine_type` L/M/H bakarrik onartzen du: mota berriko irakurketak baztertu egingo lirateke (422), eta Telegram-era errore-abisua iritsiko litzateke. Gutxienez, arazoa ez litzateke isilik geratuko. Konponbidea:

- **Eredua monitorizatu:** `cnc-alertak` dashboard-ak ekoizpeneko recall-a erakusten du zuzenean (gaur 0,91). Jaisten bada, eredua okertzen ari da.
- **Berriz entrenatu** mota berriaren datuekin, etiketatutako hutsegite nahikoa daudenean, eta API-ari mota berria gehitu.
- Gure diseinuak entrenamendua (offline) eta inferentzia (API) bereizten dituenez, nahikoa da `.pkl` berria gordetzea eta API-a berrabiaraztea.
