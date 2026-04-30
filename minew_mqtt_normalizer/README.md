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

Daarnaast worden generieke G1-velden voor MSP01 PIR/motion, PS/phototransistor, occupancy, deurstatus, ToF-afstand, radar-aantal-personen en asset-repeater nearest-beacon meegenomen wanneer ze in de JSON voorkomen.

## S4 Door Sensor

G1 firmware kan de S4 als `type: cb` publiceren met velden zoals `unlocked`, `uninstalled` en `triggered`. De normalizer zet dit om naar `door_open`, `tamper`, `installed` en `triggered` binary sensors.
