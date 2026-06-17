import json
import pathlib
import importlib.util
import sys
import types

# Test runner shim: paho-mqtt is installed inside the Home Assistant add-on
# container, but may not exist in this local packaging environment.
paho = types.ModuleType("paho")
paho_mqtt = types.ModuleType("paho.mqtt")
paho_client = types.ModuleType("paho.mqtt.client")
paho_client.Client = object
paho_mqtt.client = paho_client
paho.mqtt = paho_mqtt
sys.modules.setdefault("paho", paho)
sys.modules.setdefault("paho.mqtt", paho_mqtt)
sys.modules.setdefault("paho.mqtt.client", paho_client)

app_path = pathlib.Path('minew_mqtt_normalizer/app.py').resolve()
spec = importlib.util.spec_from_file_location('app', app_path)
app = importlib.util.module_from_spec(spec)
sys.modules['app'] = app
spec.loader.exec_module(app)

payload = json.loads(pathlib.Path('sample_payload.json').read_text())
options = {
    'allowed_device_macs': [
        'c3000%',
        'ac233fae2d75',
    ],
    'ignored_device_macs': [],
    'publish_frame_event_sensors': True,
}
states = {}
for adv in payload['adv']:
    mac = app.normalize_mac(adv['mac'])
    if not app.should_process_mac(mac, options):
        continue
    state = states.setdefault(mac, {})
    app.update_from_adv(state, adv, payload, {
        'c3000028b951': 'MSP01',
        'c300004563a0': 'MBM02',
        'ac233fae2d75': 'Plus',
        'c3000047a97f': 'S4 Door Sensor',
        'c30000191fad': 'MSR01 Radar',
    }, {
        'c30000191fad': 'MSR01-A Personnel Radar',
    }, options)

assert app.should_process_mac('c3000028b951', options) is True
assert app.should_process_mac('C3:00:00:28:B9:51', options) is True
assert app.should_process_mac('ffffffffffff', options) is False
assert 'ffffffffffff' not in states
assert states['c30000191fad']['people_count'] == 1
assert states['c30000191fad']['presence'] is True
assert states['c30000191fad']['person_1_z'] == 1.7
assert states['c3000047a97f']['door_open'] is True
print(json.dumps(states, indent=2, ensure_ascii=False))
