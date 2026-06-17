# Minew MQTT Normalizer

Normaliseert Minew G1 BLE MQTT payloads naar schone MQTT state-topics met Home Assistant MQTT discovery.

## Input

Standaard luistert de add-on op:

```text
/gw/+/status
```

Voorbeeld:

```json
{
  "tm": "2026-04-29T17:12:02.288Z",
  "gw": "ac233fc1eccc",
  "seq": 645,
  "adv": [
    {
      "type": "ht",
      "temperature": 21.1,
      "humidity": 36.67,
      "battery": 31,
      "rssi": -56,
      "mac": "c3000028b951"
    }
  ]
}
```

## Output

Per BLE apparaat:

```text
minew/g1/<gateway-mac>/device/<ble-mac>/state
```

Bijvoorbeeld:

```json
{
  "mac": "c3000028b951",
  "gateway": "ac233fc1eccc",
  "temperature": 21.1,
  "humidity": 36.67,
  "battery_percent": 31,
  "rssi": -56,
  "frames": ["ht"]
}
```

## Ondersteunde frame-types

- `info`
- `ht`
- `temp`
- `ib`, `ibeacon`, `fake_ib`, `fake_ibeacon`
- `uid`
- `url`
- `tlm`
- `acc`, `axis`, `acc_axis`
- `acc_gyro`, `accelerometer_gyroscope`, `gyro`
- `mag`, `magnetometer`
- `lux`, `light`, `light_lux`
- `pressure`, `digital_pressure`
- `tvoc`
- `pir`, `pir_sensor`, `pir_alarm`
- `vibration`
- `photoresistance`, `ps`, `phototransistor`
- `tamper`, `tamper_proof`
- `cb`, `combination`, `combination_frame` voor S4 deur-sensoren
- `lo`, `location`, `radar`, `radar_coordinate` voor MSR01-A radar/person-coordinate frames
- `info_v3` voor Minew Connect V3 apparaatinfo

Daarnaast worden generieke G1-velden voor MSP01 PIR/motion, PS/phototransistor, occupancy, deurstatus, ToF-afstand, radar-aantal-personen en asset-repeater nearest-beacon meegenomen wanneer ze in de JSON voorkomen.

## MAC-filter / allowlist

De Minew G1 kan veel BLE-apparaten uit de omgeving zien. Gebruik daarom `allowed_device_macs` om alleen gewenste apparaten naar Home Assistant te publiceren.

Exacte MAC-adressen en wildcards werken allebei:

```yaml
allowed_device_macs:
  - "c3000%"        # alles dat met C3000 begint
  - "ac233fae2d75"  # exacte MAC voor Plus
ignored_device_macs:
  - "c3000deadbeef" # optioneel: specifieke uitzondering
```

Ondersteunde wildcards:

- `%` of `*` = willekeurige rest/tekens, bijvoorbeeld `c3000%`
- `?` = precies één teken

`ignored_device_macs` heeft voorrang op `allowed_device_macs`.

## S4 Door Sensor

G1 firmware kan de S4 als `type: cb` publiceren met velden zoals `unlocked`, `uninstalled` en `triggered`. De normalizer zet dit om naar `door_open`, `tamper`, `installed` en `triggered` binary sensors.

## MSP01 PIR event-only frames

Sommige G1 firmware publiceert een MSP01 PIR-alarm als alleen:

```json
{"type":"pir","mac":"...","rssi":-55}
```

zonder extra `pir:true` of `detected:true`. Vanaf v0.2.4 wordt de aanwezigheid van zo'n `pir` frame zelf als motion-event behandeld. De `pir` en `motion` binary sensors krijgen een korte `off_delay`, zodat ze na het event automatisch terugvallen naar `off`.

## MSR01-A Radar / Personnel coordinates

G1 firmware kan de MSR01-A radar publiceren als `type: lo` met bijvoorbeeld:

```json
{"type":"lo","people":1,"axis":[{"x":-0.2,"y":-0.1,"z":1.7}],"mac":"c30000191fad"}
```

Vanaf v0.2.5 zet de normalizer dit om naar `people_count`, `occupancy`, `presence` en `person_1_x/y/z` t/m `person_5_x/y/z`.

`type: info_v3` wordt gebruikt voor apparaatinfo zoals `battery`, `ver`, `screen` en `product`.
