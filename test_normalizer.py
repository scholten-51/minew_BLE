import json
import pathlib
import importlib.util
import sys

app_path = pathlib.Path('minew_mqtt_normalizer/app.py').resolve()
spec = importlib.util.spec_from_file_location('app', app_path)
app = importlib.util.module_from_spec(spec)
sys.modules['app'] = app
spec.loader.exec_module(app)

payload = json.loads(pathlib.Path('sample_payload.json').read_text())
states = {}
for adv in payload['adv']:
    mac = app.normalize_mac(adv['mac'])
    state = states.setdefault(mac, {})
    app.update_from_adv(state, adv, payload, {
        'c3000028b951': 'MSP01',
        'c300004563a0': 'MBM02',
        'ac233fae2d75': 'Plus',
    }, {})

print(json.dumps(states, indent=2, ensure_ascii=False))
