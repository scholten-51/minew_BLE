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

## Versie 0.2.0

Deze versie is uitgebreid op basis van de Minew BeaconPlus Frame Definition V005.

Extra ondersteuning:

- iBeacon en Fake iBeacon, inclusief batterij indien aanwezig
- Eddystone UID, URL en TLM
- HT en TEMP frames
- ACC, ACC+Gyro en magnetometer frames
- Lux, pressure/weight, digital pressure, TVOC en photoresistance frames
- PIR, vibration en tamper proof frames
- Generieke velden voor deur, occupancy, ToF, radar en asset repeater output wanneer de G1 firmware die al decodeert

De add-on doet geen low-level BLE raw-byte parsing; hij normaliseert wat de Minew G1 al als JSON in `adv[]` publiceert.
