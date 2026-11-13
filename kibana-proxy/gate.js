(function () {
  try {
    var ss = sessionStorage;
    var nav = (performance.getEntriesByType('navigation')[0] || {}).type;
    var irten = ss.getItem('cncHide');
    ss.removeItem('cncHide');
    if (!ss.getItem('cncGate') || (irten && nav !== 'reload')) {
      ss.removeItem('cncGate');
      location.replace('/__auth/logout');
      return;
    }
    window.addEventListener('pagehide', function () { ss.setItem('cncHide', '1'); });
    window.addEventListener('pageshow', function (e) {
      if (e.persisted) { ss.removeItem('cncGate'); location.replace('/__auth/logout'); }
    });
  } catch (e) {}
})();