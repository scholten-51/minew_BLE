# Minew MQTT Normalizer Add-ons

Home Assistant add-on repository voor de Minew MQTT Normalizer.

## Installeren in Home Assistant

1. Ga naar **Settings -> Add-ons -> Add-on Store**.
2. Klik rechtsboven op de drie puntjes.
3. Kies **Repositories**.
4. Voeg de GitHub URL van deze repository toe.
5. Klik **Add**.
6. Installeer **Minew MQTT Normalizer** vanuit de add-on store.

## Wat doet de add-on?

De add-on luistert naar ruwe Minew G1 MQTT berichten zoals:

```text
/gw/ac233fc1eccc/status
```

En publiceert per BLE MAC-adres een schoon state-topic:

```text
minew/g1/ac233fc1eccc/device/<mac>/state
```

Daarnaast publiceert hij Home Assistant MQTT discovery topics onder `homeassistant/...`, zodat devices automatisch zichtbaar worden.

## Versie 0.2.7

Deze versie voegt bovenop v0.2.6 wildcard filtering toe voor BLE MAC-adressen. De G1 ziet vaak honderden BLE-apparaten uit de omgeving; met `allowed_device_macs` publiceert de add-on alleen apparaten die je bewust toestaat.

Extra ondersteuning:

- iBeacon en Fake iBeacon, inclusief batterij indien aanwezig
- Eddystone UID, URL en TLM
- HT en TEMP frames
- ACC, ACC+Gyro en magnetometer frames
- Lux, pressure/weight, digital pressure, TVOC en photoresistance frames
- PIR, vibration en tamper proof frames
- Verbeterde MSP01 ondersteuning voor PIR/motion, inclusief event-only PIR frames, HT, ACC en PS/phototransistor/lichtvelden
- S4 Door Sensor: `unlocked` naar deur open/dicht, `uninstalled` naar tamper/geinstalleerd en `triggered` als extra trigger-status
- MSR01-A Radar: `people` naar aanwezigheid/personentelling, `axis` naar person_1 t/m person_5 X/Y/Z coordinaten, en `info_v3` naar batterij/firmware/product/screen
- MAC-filter met wildcards: `c3000%` of `c3000*` staat alle BLE MAC-adressen toe die met C3000 beginnen; exacte MACs blijven ook ondersteund

## MAC-filter

Standaard publiceert v0.2.7 alleen toegestane apparaten naar Home Assistant. Exacte MAC-adressen en wildcard-patronen werken allebei:

```yaml
allowed_device_macs:
  - "c3000%"        # alles dat met C3000 begint
  - "ac233fae2d75"  # exacte MAC voor Plus
ignored_device_macs: []
```

Wildcards mogen met `%` of `*`. `ignored_device_macs` wint altijd van `allowed_device_macs`, zodat je bijvoorbeeld `c3000%` kunt toestaan maar één specifiek apparaat toch kunt blokkeren.

De add-on doet geen low-level BLE raw-byte parsing; hij normaliseert wat de Minew G1 al als JSON in `adv[]` publiceert.
