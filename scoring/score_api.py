from pydantic import Field
from typing import Literal
from contextlib import asynccontextmanager
import os
import time
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import joblib
import pandas as pd
import numpy as np
from pydantic import BaseModel

load_dotenv()

EREDU_BIDE = "models/cnc_failure_model.pkl"
eredua = None
KELVIN = 273.15


@asynccontextmanager
async def lifespan(app: FastAPI):
    global eredua
    if os.path.exists(EREDU_BIDE):
        eredua = joblib.load(EREDU_BIDE)
        print("Eredua ondo kargatu da memoriak.")
    else:
        print(f"KONTUZ: Ez da fitxategia aurkitu: {EREDU_BIDE}.")
    yield
    eredua = None
    print("Zerbitzaria itzaltzen")


app = FastAPI(
    title="CNC Guard Scoring API",
    version="1.0.0",
    lifespan=lifespan,
    root_path="/proxy/8000"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.middleware("http")
async def denbora_neurtu(eskaera, deitu_hurrengoa):
    hasiera = time.time()
    erantzuna = await deitu_hurrengoa(eskaera)
    erantzuna.headers["X-prozesatze-denbora"] = str(
        round(time.time() - hasiera, 4)
    )
    return erantzuna


TOKEN_BALIOA = os.getenv("API_TOKEN", "admin")
segurtasuna = HTTPBearer()


def token_egiaztatu(
    kredentzialak: HTTPAuthorizationCredentials = Depends(segurtasuna),
):
    if kredentzialak.credentials != TOKEN_BALIOA:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token baliogabea"
        )
    return kredentzialak.credentials


class SentsoreEskaera(BaseModel):
    timestamp: str
    machine_id: str
    machine_type: Literal["L", "M", "H"]
    air_temp_K: float = Field(..., ge=250, le=350)
    process_temp_K: float = Field(..., ge=250, le=400)
    rpm: int = Field(..., gt=0)
    torque_Nm: float = Field(..., ge=0)
    tool_wear_min: int = Field(..., ge=0)


class PredikzioErantzuna(BaseModel):
    timestamp: str
    machine_id: str
    machine_type: str
    air_temp_C: float
    process_temp_C: float
    temp_diff: float
    rpm: int
    torque_Nm: float
    power_est: float
    tool_wear_min: int
    machine_failure: bool
    failure_predicted: bool
    failure_probability: float
    alert_level: str


@app.get("/health")
def health():
    if eredua is None:
        raise HTTPException(status_code=503, detail="Eredua ez dago kargatua")
    return {"status": "ok", "model_loaded": True}


@app.post("/predict", response_model=PredikzioErantzuna, dependencies=[Depends(token_egiaztatu)])
def predict(data: SentsoreEskaera):
    if eredua is None:
        raise HTTPException(status_code=503, detail="Eredua ez dago prest")

    try:
        air_c = data.air_temp_K - KELVIN
        process_c = data.process_temp_K - KELVIN

        features_df = pd.DataFrame([{
            "machine_type": data.machine_type,
            "air_temp_C": air_c,
            "process_temp_C": process_c,
            "temp_diff": process_c - air_c,
            "rpm": data.rpm,
            "torque_Nm": data.torque_Nm,
            "power_est": data.torque_Nm * data.rpm * 2 * np.pi / 60,
            "tool_wear_min": data.tool_wear_min
        }])

        prob = float(eredua.predict_proba(features_df)[0][1])

        if prob >= 0.70:
            alert = "LARRIA"
        elif prob >= 0.15:
            alert = "KONTUZ"
        else:
            alert = "OK"

        return PredikzioErantzuna(
            timestamp=data.timestamp,
            machine_id=data.machine_id,
            machine_type=data.machine_type,
            air_temp_C=features_df.iloc[0].air_temp_C,
            process_temp_C=features_df.iloc[0].process_temp_C,
            temp_diff=features_df.iloc[0].temp_diff,
            rpm=data.rpm,
            torque_Nm=data.torque_Nm,
            power_est=features_df.iloc[0].power_est,
            tool_wear_min=data.tool_wear_min,
            machine_failure=data.machine_failure,
            failure_predicted=prob >= 0.15,
            failure_probability=round(prob, 4),
            alert_level=alert
        )

    except Exception as e:
        print(f"Errorea predikzioan: {e}")
        raise HTTPException(status_code=500, detail=str(e))