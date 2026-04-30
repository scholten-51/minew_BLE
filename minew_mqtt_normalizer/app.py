#!/usr/bin/env python3
"""Minew G1 MQTT normalizer for Home Assistant.

Subscribes to Minew G1 raw MQTT status payloads like /gw/<mac>/status and
publishes one clean retained JSON state topic per BLE MAC address plus
Home Assistant MQTT discovery topics.

The normalizer works with already-decoded Minew G1 payloads (adv[] rows with
fields such as type, mac, temperature, humidity, battery, rssi, uuid, major,
minor, etc.). It keeps the raw BLE parsing inside the G1/gateway and normalizes
whatever the gateway exposes into Home Assistant friendly state objects.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import paho.mqtt.client as mqtt

OPTIONS_PATH = "/data/options.json"
DEFAULT_OPTIONS: dict[str, Any] = {
    "mqtt_host": "core-mosquitto",
    "mqtt_port": 1883,
    "mqtt_username": "",
    "mqtt_password": "",
    "raw_topic": "/gw/+/status",
    "state_topic_prefix": "minew",
    "discovery_prefix": "homeassistant",
    "retain": True,
    "qos": 0,
    "availability_timeout_seconds": 300,
    "publish_gateway": True,
    "publish_frame_event_sensors": True,
    "log_level": "info",
    "device_name_overrides": [],
    "device_model_overrides": [],
}

SENSOR_DEFS: dict[str, dict[str, Any]] = {
    # Common values
    "temperature": {"name": "Temperature", "unit": "°C", "device_class": "temperature", "state_class": "measurement"},
    "humidity": {"name": "Humidity", "unit": "%", "device_class": "humidity", "state_class": "measurement"},
    "battery_percent": {"name": "Battery", "unit": "%", "device_class": "battery", "state_class": "measurement"},
    "battery_mv": {"name": "Battery Voltage", "unit": "mV", "device_class": "voltage", "state_class": "measurement"},
    "rssi": {"name": "RSSI", "unit": "dBm", "device_class": "signal_strength", "state_class": "measurement"},
    "rssi_at_xm": {"name": "RSSI at Xm", "unit": "dBm", "device_class": "signal_strength", "state_class": "measurement"},
    "tx_power": {"name": "TX Power", "unit": "dBm", "device_class": "signal_strength", "state_class": "measurement"},
    "device_type": {"name": "Detected Type", "icon": "mdi:bluetooth"},
    "name": {"name": "Advertised Name", "icon": "mdi:tag-text-outline"},
    "frames": {"name": "Frames", "icon": "mdi:format-list-bulleted"},
    "last_seen": {"name": "Last Seen", "device_class": "timestamp"},
    "last_ibeacon_seen": {"name": "Last iBeacon Seen", "device_class": "timestamp"},
    "last_url_seen": {"name": "Last URL Seen", "device_class": "timestamp"},
    "last_uid_seen": {"name": "Last UID Seen", "device_class": "timestamp"},
    "last_pir_seen": {"name": "Last PIR Seen", "device_class": "timestamp"},
    "last_tamper_seen": {"name": "Last Tamper Seen", "device_class": "timestamp"},
    "last_vibration_seen": {"name": "Last Vibration Seen", "device_class": "timestamp"},
    "last_update_unix": {"name": "Last Update Unix", "unit": "s", "device_class": "timestamp"},

    # Beacon formats
    "major": {"name": "iBeacon Major"},
    "minor": {"name": "iBeacon Minor"},
    "uuid": {"name": "iBeacon UUID", "icon": "mdi:identifier"},
    "url": {"name": "URL", "icon": "mdi:link"},
    "namespace_id": {"name": "UID Namespace", "icon": "mdi:identifier"},
    "instance_id": {"name": "UID Instance", "icon": "mdi:identifier"},
    "telemetry_temperature": {"name": "Telemetry Temperature", "unit": "°C", "device_class": "temperature", "state_class": "measurement"},
    "adv_count": {"name": "Advertising Count", "state_class": "total_increasing", "icon": "mdi:counter"},
    "seconds_count": {"name": "Seconds Count", "unit": "s", "device_class": "duration", "state_class": "total_increasing"},

    # BeaconPlus V005 decoded fields
    "lux": {"name": "Illuminance", "unit": "lx", "device_class": "illuminance", "state_class": "measurement"},
    "pressure_gram": {"name": "Pressure Weight", "unit": "g", "state_class": "measurement", "icon": "mdi:scale"},
    "pressure_hpa": {"name": "Pressure", "unit": "hPa", "device_class": "atmospheric_pressure", "state_class": "measurement"},
    "tvoc_ppb": {"name": "TVOC", "unit": "ppb", "device_class": "volatile_organic_compounds_parts", "state_class": "measurement"},
    "photo_lumens": {"name": "Photoresistance Lumens", "unit": "lm", "state_class": "measurement", "icon": "mdi:white-balance-sunny"},
    "vibration_timestamp": {"name": "Vibration Timestamp", "icon": "mdi:timer-outline"},

    # Axes / motion
    "x_axis": {"name": "X Axis", "unit": "g", "state_class": "measurement", "icon": "mdi:axis-x-arrow"},
    "y_axis": {"name": "Y Axis", "unit": "g", "state_class": "measurement", "icon": "mdi:axis-y-arrow"},
    "z_axis": {"name": "Z Axis", "unit": "g", "state_class": "measurement", "icon": "mdi:axis-z-arrow"},
    "acc_x": {"name": "ACC X", "unit": "g", "state_class": "measurement", "icon": "mdi:axis-x-arrow"},
    "acc_y": {"name": "ACC Y", "unit": "g", "state_class": "measurement", "icon": "mdi:axis-y-arrow"},
    "acc_z": {"name": "ACC Z", "unit": "g", "state_class": "measurement", "icon": "mdi:axis-z-arrow"},
    "gyro_x_dps": {"name": "Gyro X", "unit": "dps", "state_class": "measurement", "icon": "mdi:axis-x-rotate-clockwise"},
    "gyro_y_dps": {"name": "Gyro Y", "unit": "dps", "state_class": "measurement", "icon": "mdi:axis-y-rotate-clockwise"},
    "gyro_z_dps": {"name": "Gyro Z", "unit": "dps", "state_class": "measurement", "icon": "mdi:axis-z-rotate-clockwise"},
    "mag_x_10mg": {"name": "Magnetometer X", "unit": "10mG", "state_class": "measurement", "icon": "mdi:magnet"},
    "mag_y_10mg": {"name": "Magnetometer Y", "unit": "10mG", "state_class": "measurement", "icon": "mdi:magnet"},
    "mag_z_10mg": {"name": "Magnetometer Z", "unit": "10mG", "state_class": "measurement", "icon": "mdi:magnet"},

    # Occupancy / door / radar / repeater generic decoded fields
    "distance_mm": {"name": "Distance", "unit": "mm", "device_class": "distance", "state_class": "measurement"},
    "people_count": {"name": "People Count", "state_class": "measurement", "icon": "mdi:account-group"},
    "open_count": {"name": "Open Count", "state_class": "total_increasing", "icon": "mdi:door-open"},
    "close_count": {"name": "Close Count", "state_class": "total_increasing", "icon": "mdi:door-closed"},
    "tamper_count": {"name": "Tamper Count", "state_class": "total_increasing", "icon": "mdi:shield-alert"},
    "occupancy_count": {"name": "Occupancy Count", "state_class": "total_increasing", "icon": "mdi:counter"},
    "dismantle_count": {"name": "Dismantle Count", "state_class": "total_increasing", "icon": "mdi:shield-alert"},
    "nearest_beacon_mac": {"name": "Nearest Beacon MAC", "icon": "mdi:map-marker-radius"},
    "nearest_beacon_rssi": {"name": "Nearest Beacon RSSI", "unit": "dBm", "device_class": "signal_strength", "state_class": "measurement"},
}

BINARY_SENSOR_DEFS: dict[str, dict[str, Any]] = {
    "occupancy": {"name": "Occupancy", "device_class": "occupancy"},
    "pir": {"name": "PIR", "device_class": "motion"},
    "tamper": {"name": "Tamper", "device_class": "tamper"},
    "low_battery": {"name": "Low Battery", "device_class": "battery"},
    "door_open": {"name": "Door", "device_class": "door"},
    "motion": {"name": "Motion", "device_class": "motion"},
    "vibration": {"name": "Vibration", "device_class": "vibration"},
}

EVENT_FRAME_FIELDS = {
    "ib": "last_ibeacon_seen",
    "ibeacon": "last_ibeacon_seen",
    "url": "last_url_seen",
    "uid": "last_uid_seen",
    "pir": "last_pir_seen",
    "vibration": "last_vibration_seen",
    "tamper": "last_tamper_seen",
    "tamper_proof": "last_tamper_seen",
}


def load_options() -> dict[str, Any]:
    options = DEFAULT_OPTIONS.copy()
    if os.path.exists(OPTIONS_PATH):
        with open(OPTIONS_PATH, "r", encoding="utf-8") as f:
            options.update(json.load(f))
    # Allow quick local testing outside Home Assistant.
    for key in list(DEFAULT_OPTIONS):
        env_key = f"MINEW_{key.upper()}"
        if env_key in os.environ:
            raw = os.environ[env_key]
            if isinstance(DEFAULT_OPTIONS[key], bool):
                options[key] = raw.lower() in {"1", "true", "yes", "on"}
            elif isinstance(DEFAULT_OPTIONS[key], int):
                options[key] = int(raw)
            else:
                options[key] = raw
    return options


def parse_overrides(rows: list[str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows or []:
        if "=" not in row:
            continue
        mac, value = row.split("=", 1)
        mac = normalize_mac(mac)
        value = value.strip()
        if mac and value:
            result[mac] = value
    return result


def normalize_mac(value: Any) -> str:
    return str(value or "").lower().replace(":", "").replace("-", "").strip()


def safe_id(value: Any) -> str:
    text = str(value or "").lower()
    allowed = []
    for ch in text:
        allowed.append(ch if ch.isalnum() else "_")
    compact = "_".join(part for part in "".join(allowed).split("_") if part)
    return compact or "unknown"


def coerce_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "on", "open", "opened", "occupied", "yes", "detected", "moving", "alarm", "demolished", "in_vibration", "in vibration"}:
        return True
    if text in {"0", "false", "off", "closed", "unoccupied", "no", "normal", "none", "clear", "no vibration", "no_vibration"}:
        return False
    return None


def latest(existing: Any, candidate: Any) -> Any:
    return candidate if candidate is not None else existing


@dataclass
class Runtime:
    options: dict[str, Any]
    client: mqtt.Client
    running: bool = True
    discovered: set[str] = field(default_factory=set)
    last_seen_mono: dict[str, float] = field(default_factory=dict)
    device_names: dict[str, str] = field(default_factory=dict)
    device_models: dict[str, str] = field(default_factory=dict)
    states: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def state_prefix(self) -> str:
        return str(self.options["state_topic_prefix"]).strip("/")

    @property
    def discovery_prefix(self) -> str:
        return str(self.options["discovery_prefix"]).strip("/")

    @property
    def qos(self) -> int:
        return int(self.options.get("qos", 0))

    @property
    def retain(self) -> bool:
        return bool(self.options.get("retain", True))

    def publish_json(self, topic: str, payload: dict[str, Any], retain: bool | None = None) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        self.client.publish(topic, data, qos=self.qos, retain=self.retain if retain is None else retain)
        logging.debug("published %s %s", topic, data)

    def publish_text(self, topic: str, payload: str, retain: bool | None = None) -> None:
        self.client.publish(topic, payload, qos=self.qos, retain=self.retain if retain is None else retain)
        logging.debug("published %s %s", topic, payload)

    def state_topic(self, gateway: str, mac: str) -> str:
        return f"{self.state_prefix}/g1/{gateway}/device/{mac}/state"

    def availability_topic(self, gateway: str, mac: str) -> str:
        return f"{self.state_prefix}/g1/{gateway}/device/{mac}/availability"

    def gateway_state_topic(self, gateway: str) -> str:
        return f"{self.state_prefix}/g1/{gateway}/state"

    def gateway_availability_topic(self, gateway: str) -> str:
        return f"{self.state_prefix}/g1/{gateway}/availability"

    def device_info(self, gateway: str, mac: str, state: dict[str, Any]) -> dict[str, Any]:
        name = state.get("name") or self.device_names.get(mac) or f"Minew {mac}"
        model = self.device_models.get(mac) or state.get("model") or infer_model(state) or "BLE device"
        return {
            "identifiers": [f"minew_{mac}"],
            "name": name,
            "manufacturer": "Minew",
            "model": model,
            "via_device": f"minew_g1_{gateway}",
        }

    def gateway_device_info(self, gateway: str) -> dict[str, Any]:
        return {
            "identifiers": [f"minew_g1_{gateway}"],
            "name": f"Minew G1 {gateway}",
            "manufacturer": "Minew",
            "model": "G1 BLE Gateway",
        }

    def publish_gateway_discovery(self, gateway: str) -> None:
        if not self.options.get("publish_gateway", True):
            return
        topic = f"{self.discovery_prefix}/sensor/minew_{gateway}_seq/config"
        if topic in self.discovered:
            return
        payload = {
            "name": "Sequence",
            "unique_id": f"minew_{gateway}_seq",
            "state_topic": self.gateway_state_topic(gateway),
            "value_template": "{{ value_json.seq }}",
            "json_attributes_topic": self.gateway_state_topic(gateway),
            "availability_topic": self.gateway_availability_topic(gateway),
            "icon": "mdi:counter",
            "device": self.gateway_device_info(gateway),
        }
        self.publish_json(topic, payload, retain=True)
        self.discovered.add(topic)

    def publish_entity_discovery(self, gateway: str, mac: str, state: dict[str, Any]) -> None:
        node = f"minew_{gateway}_{mac}"
        state_topic = self.state_topic(gateway, mac)
        availability_topic = self.availability_topic(gateway, mac)
        dev = self.device_info(gateway, mac, state)

        for field_name, meta in SENSOR_DEFS.items():
            if field_name not in state:
                continue
            # last_update_unix is numeric, but Home Assistant timestamp sensors expect ISO strings.
            if field_name == "last_update_unix":
                continue
            object_id = f"{node}_{safe_id(field_name)}"
            topic = f"{self.discovery_prefix}/sensor/{object_id}/config"
            if topic in self.discovered:
                continue
            payload = {
                "name": meta.get("name", field_name.replace("_", " ").title()),
                "unique_id": object_id,
                "state_topic": state_topic,
                "value_template": f"{{{{ value_json.{field_name} }}}}",
                "json_attributes_topic": state_topic,
                "availability_topic": availability_topic,
                "device": dev,
            }
            if "unit" in meta:
                payload["unit_of_measurement"] = meta["unit"]
            if "device_class" in meta:
                payload["device_class"] = meta["device_class"]
            if "state_class" in meta:
                payload["state_class"] = meta["state_class"]
            if "icon" in meta:
                payload["icon"] = meta["icon"]
            self.publish_json(topic, payload, retain=True)
            self.discovered.add(topic)

        for field_name, meta in BINARY_SENSOR_DEFS.items():
            if field_name not in state:
                continue
            object_id = f"{node}_{safe_id(field_name)}"
            topic = f"{self.discovery_prefix}/binary_sensor/{object_id}/config"
            if topic in self.discovered:
                continue
            payload = {
                "name": meta.get("name", field_name.replace("_", " ").title()),
                "unique_id": object_id,
                "state_topic": state_topic,
                "value_template": f"{{% if value_json.{field_name} %}}ON{{% else %}}OFF{{% endif %}}",
                "payload_on": "ON",
                "payload_off": "OFF",
                "json_attributes_topic": state_topic,
                "availability_topic": availability_topic,
                "device": dev,
            }
            if "device_class" in meta:
                payload["device_class"] = meta["device_class"]
            self.publish_json(topic, payload, retain=True)
            self.discovered.add(topic)

    def process_payload(self, payload: dict[str, Any], topic: str) -> None:
        gateway = normalize_mac(payload.get("gw")) or gateway_from_topic(topic)
        if not gateway:
            logging.warning("No gateway id found in topic=%s payload=%s", topic, payload)
            return

        adv_rows = payload.get("adv") if isinstance(payload.get("adv"), list) else []
        per_message: dict[str, dict[str, Any]] = {}

        for adv in adv_rows:
            if not isinstance(adv, dict):
                continue
            mac = normalize_mac(adv.get("mac"))
            if not mac:
                continue
            state = per_message.setdefault(mac, self.states.get(mac, {}).copy())
            update_from_adv(state, adv, payload, self.device_names, self.device_models, self.options)

        if self.options.get("publish_gateway", True):
            self.publish_gateway_discovery(gateway)
            self.publish_text(self.gateway_availability_topic(gateway), "online", retain=True)
            self.publish_json(self.gateway_state_topic(gateway), {
                "gateway": gateway,
                "seq": payload.get("seq"),
                "last_seen": payload.get("tm"),
                "adv_count": len(adv_rows),
            }, retain=True)

        now = time.monotonic()
        for mac, state in per_message.items():
            state["mac"] = mac
            state["gateway"] = gateway
            state["last_update_unix"] = int(time.time())
            self.states[mac] = state
            key = f"{gateway}:{mac}"
            self.last_seen_mono[key] = now
            self.publish_entity_discovery(gateway, mac, state)
            self.publish_text(self.availability_topic(gateway, mac), "online", retain=True)
            self.publish_json(self.state_topic(gateway, mac), state, retain=True)

    def check_offline(self) -> None:
        timeout = int(self.options.get("availability_timeout_seconds", 300))
        now = time.monotonic()
        for key, seen in list(self.last_seen_mono.items()):
            if now - seen <= timeout:
                continue
            gateway, mac = key.split(":", 1)
            self.publish_text(self.availability_topic(gateway, mac), "offline", retain=True)
            del self.last_seen_mono[key]


def infer_model(state: dict[str, Any]) -> str | None:
    name = str(state.get("name") or "").strip()
    if name:
        return name
    frames = set(state.get("frames") or [])
    if "ht" in frames:
        return "HT sensor"
    if "temp" in frames:
        return "Temperature sensor"
    if {"ib", "url", "tlm", "info", "uid"}.intersection(frames):
        return "BeaconPlus"
    if "pir" in frames:
        return "PIR sensor"
    if "tamper" in frames or "tamper_proof" in frames:
        return "Tamper sensor"
    if "vibration" in frames:
        return "Vibration sensor"
    if "distance_mm" in state or "occupancy" in state:
        return "Occupancy sensor"
    if "nearest_beacon_mac" in state:
        return "Asset repeater"
    return None


def update_from_adv(
    state: dict[str, Any],
    adv: dict[str, Any],
    root: dict[str, Any],
    name_overrides: dict[str, str],
    model_overrides: dict[str, str],
    options: dict[str, Any] | None = None,
) -> None:
    mac = normalize_mac(adv.get("mac"))
    adv_type = str(adv.get("type") or "unknown").lower()
    frames = set(state.get("frames") or [])
    frames.add(adv_type)
    state["frames"] = sorted(frames)
    state["device_type"] = adv_type
    state["rssi"] = latest(state.get("rssi"), adv.get("rssi"))
    state["last_seen"] = adv.get("tm") or root.get("tm") or state.get("last_seen")

    if mac in name_overrides:
        state["name"] = name_overrides[mac]
    if mac in model_overrides:
        state["model"] = model_overrides[mac]

    # Store event timestamps instead of latched event booleans for click-like frames.
    if (options or {}).get("publish_frame_event_sensors", True):
        event_field = EVENT_FRAME_FIELDS.get(adv_type)
        if event_field:
            state[event_field] = adv.get("tm") or root.get("tm") or state.get(event_field)

    # Common fields found in several decoded G1 firmware variants.
    copy_number(state, adv, "tx_power", "tx_power")
    copy_number(state, adv, "rssi_at_xm", "rssi_at_xm")

    if adv_type == "info":
        if adv.get("name") is not None and mac not in name_overrides:
            state["name"] = adv.get("name")
        if adv.get("battery") is not None:
            state["battery_percent"] = adv.get("battery")

    elif adv_type in {"ht", "temperature_humidity"}:
        copy_number(state, adv, "temperature", "temperature")
        copy_number(state, adv, "humidity", "humidity")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"temp", "temperature"}:
        copy_number(state, adv, "temperature", "temperature")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"ib", "ibeacon", "fake_ib", "fake_ibeacon"}:
        for key in ("uuid", "major", "minor"):
            if adv.get(key) is not None:
                state[key] = adv.get(key)
        copy_number(state, adv, "battery", "battery_percent")
        state["last_ibeacon_seen"] = adv.get("tm") or root.get("tm") or state.get("last_ibeacon_seen")

    elif adv_type == "uid":
        for src, dst in (("namespace", "namespace_id"), ("namespace_id", "namespace_id"), ("instance", "instance_id"), ("instance_id", "instance_id")):
            if adv.get(src) is not None:
                state[dst] = adv.get(src)
        state["last_uid_seen"] = adv.get("tm") or root.get("tm") or state.get("last_uid_seen")

    elif adv_type == "url":
        if adv.get("url") is not None:
            state["url"] = adv.get("url")
        state["last_url_seen"] = adv.get("tm") or root.get("tm") or state.get("last_url_seen")
        # Keep URL as metadata. Do not infer motion just because a URL contains "pir";
        # many Minew devices use URL frames as identifiers rather than live states.

    elif adv_type == "tlm":
        # Eddystone TLM battery is battery voltage in mV, not percentage.
        copy_number(state, adv, "battery", "battery_mv")
        copy_number(state, adv, "battery_mv", "battery_mv")
        copy_number(state, adv, "temperature", "telemetry_temperature")
        copy_number(state, adv, "adv_cnt", "adv_count")
        copy_number(state, adv, "adv_count", "adv_count")
        copy_number(state, adv, "sec_cnt", "seconds_count")
        copy_number(state, adv, "seconds_count", "seconds_count")

    elif adv_type in {"acc", "axis", "acc_axis"}:
        copy_axes(state, adv, prefix="")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"acc_gyro", "accelerometer_gyroscope", "gyro"}:
        copy_axes(state, adv, prefix="acc_")
        copy_number(state, adv, "acc_x", "acc_x")
        copy_number(state, adv, "acc_y", "acc_y")
        copy_number(state, adv, "acc_z", "acc_z")
        copy_number(state, adv, "deg_x", "gyro_x_dps")
        copy_number(state, adv, "deg_y", "gyro_y_dps")
        copy_number(state, adv, "deg_z", "gyro_z_dps")
        copy_number(state, adv, "gyro_x", "gyro_x_dps")
        copy_number(state, adv, "gyro_y", "gyro_y_dps")
        copy_number(state, adv, "gyro_z", "gyro_z_dps")

    elif adv_type in {"mag", "magnetometer", "magnet"}:
        copy_number(state, adv, "x", "mag_x_10mg")
        copy_number(state, adv, "y", "mag_y_10mg")
        copy_number(state, adv, "z", "mag_z_10mg")
        copy_number(state, adv, "x_axis", "mag_x_10mg")
        copy_number(state, adv, "y_axis", "mag_y_10mg")
        copy_number(state, adv, "z_axis", "mag_z_10mg")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"lux", "light", "light_lux"}:
        copy_number(state, adv, "lux", "lux")
        copy_number(state, adv, "light", "lux")
        copy_number(state, adv, "light_lux", "lux")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"pressure", "weight_pressure"}:
        copy_number(state, adv, "pressure", "pressure_gram")
        copy_number(state, adv, "pressure_gram", "pressure_gram")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"digital_pressure", "barometer", "hpa"}:
        copy_number(state, adv, "pressure", "pressure_hpa")
        copy_number(state, adv, "pressure_hpa", "pressure_hpa")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type == "tvoc":
        copy_number(state, adv, "tvoc", "tvoc_ppb")
        copy_number(state, adv, "tvoc_ppb", "tvoc_ppb")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"pir", "infrared"}:
        # BeaconPlus PIR Data: 0x0000 = no infrared, 0x0001 = infrared detected.
        value = first_present(adv, ["pir", "pir_data", "infrared", "infrared_detected", "detected"])
        parsed = coerce_bool(value)
        if parsed is not None:
            state["pir"] = parsed
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"vibration", "vib"}:
        value = first_present(adv, ["vibration", "vibration_status", "status", "moving"])
        parsed = coerce_bool(value)
        if parsed is not None:
            state["vibration"] = parsed
        copy_number(state, adv, "timestamp", "vibration_timestamp")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"photo", "photoresistance", "lumens"}:
        copy_number(state, adv, "lumens", "photo_lumens")
        copy_number(state, adv, "photo_lumens", "photo_lumens")
        copy_number(state, adv, "battery", "battery_percent")

    elif adv_type in {"tamper", "tamper_proof"}:
        # BeaconPlus Tamper Proof: 0x01 = demolished, 0x00 = normal.
        value = first_present(adv, ["tamper", "tamper_proof", "tamper_status", "demolished", "status"])
        parsed = coerce_bool(value)
        if parsed is not None:
            state["tamper"] = parsed
        copy_number(state, adv, "battery", "battery_percent")

    # Generic decoded fields. These cover future G1 firmware decoders for
    # S4 door, MSD01 ToF occupancy, MSR01 radar, MBT02 repeater, etc.
    generic_number_map = {
        "distance": "distance_mm",
        "distance_mm": "distance_mm",
        "people": "people_count",
        "people_count": "people_count",
        "total_number_of_people": "people_count",
        "total_people": "people_count",
        "open_count": "open_count",
        "door_open_count": "open_count",
        "close_count": "close_count",
        "door_close_count": "close_count",
        "tamper_count": "tamper_count",
        "anti_disassembly_count": "tamper_count",
        "dismantle_count": "dismantle_count",
        "occupy_number": "occupancy_count",
        "occupancy_count": "occupancy_count",
        "nearest_beacon_rssi": "nearest_beacon_rssi",
        "strongest_rssi": "nearest_beacon_rssi",
    }
    for src, dst in generic_number_map.items():
        copy_number(state, adv, src, dst)

    generic_text_map = {
        "nearest_beacon_mac": "nearest_beacon_mac",
        "strongest_mac": "nearest_beacon_mac",
        "beacon_mac": "nearest_beacon_mac",
    }
    for src, dst in generic_text_map.items():
        if adv.get(src) is not None:
            state[dst] = normalize_mac(adv.get(src)) or adv.get(src)

    generic_bool_map = {
        "occupy": "occupancy",
        "occupied": "occupancy",
        "occupancy": "occupancy",
        "pir": "pir",
        "infrared_trigger": "pir",
        "ir_trigger": "pir",
        "dismantle": "tamper",
        "tamper": "tamper",
        "anti_disassembly": "tamper",
        "low_battery": "low_battery",
        "low-battery": "low_battery",
        "door_open": "door_open",
        "door": "door_open",
        "open": "door_open",
        "motion": "motion",
        "vibration": "vibration",
    }
    for src, dst in generic_bool_map.items():
        if src in adv:
            value = coerce_bool(adv.get(src))
            if value is not None:
                state[dst] = value


def copy_axes(state: dict[str, Any], source: dict[str, Any], prefix: str = "") -> None:
    # Several G1 firmwares use x/y/z, x_axis/y_axis/z_axis, or acc_x/acc_y/acc_z.
    mapping = [
        ("x", f"{prefix}x" if prefix else "x_axis"),
        ("y", f"{prefix}y" if prefix else "y_axis"),
        ("z", f"{prefix}z" if prefix else "z_axis"),
        ("x_axis", f"{prefix}x" if prefix else "x_axis"),
        ("y_axis", f"{prefix}y" if prefix else "y_axis"),
        ("z_axis", f"{prefix}z" if prefix else "z_axis"),
    ]
    for src, dst in mapping:
        copy_number(state, source, src, dst)


def first_present(source: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in source:
            return source.get(key)
    return None


def copy_number(state: dict[str, Any], source: dict[str, Any], src: str, dst: str) -> None:
    if source.get(src) is None:
        return
    value = source.get(src)
    if isinstance(value, (int, float)):
        state[dst] = value
        return
    try:
        state[dst] = float(value)
    except (TypeError, ValueError):
        state[dst] = value


def gateway_from_topic(topic: str) -> str:
    parts = topic.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "gw":
        return normalize_mac(parts[1])
    return ""


def mqtt_reason_code_failed(reason_code: Any) -> bool:
    """Return True when a paho-mqtt connection reason code indicates failure.

    paho-mqtt 2.x passes a ReasonCode object for VERSION2 callbacks. That
    object cannot always be converted with int(reason_code), so handle both
    the new object form and older integer/string forms.
    """
    is_failure = getattr(reason_code, "is_failure", None)
    if is_failure is not None:
        return bool(is_failure)

    value = getattr(reason_code, "value", None)
    if value is not None:
        try:
            return int(value) != 0
        except (TypeError, ValueError):
            pass

    try:
        return int(reason_code) != 0
    except (TypeError, ValueError):
        return str(reason_code).strip().lower() not in {"0", "success", "connack accepted"}


def on_connect_factory(runtime: Runtime) -> Callable[..., None]:
    def on_connect(client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any | None = None) -> None:
        if mqtt_reason_code_failed(reason_code):
            logging.error("MQTT connection failed: %s", reason_code)
            return
        raw_topic = runtime.options["raw_topic"]
        logging.info("Connected to MQTT. Subscribing to %s", raw_topic)
        client.subscribe(raw_topic, qos=runtime.qos)
    return on_connect


def on_message_factory(runtime: Runtime) -> Callable[..., None]:
    def on_message(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
        raw = message.payload.decode("utf-8", errors="replace")
        logging.debug("received %s %s", message.topic, raw)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            logging.warning("Ignoring non-JSON payload on %s: %s", message.topic, exc)
            return
        if not isinstance(payload, dict):
            logging.warning("Ignoring JSON payload that is not an object on %s", message.topic)
            return
        try:
            runtime.process_payload(payload, message.topic)
        except Exception:
            logging.exception("Failed to process payload on %s", message.topic)
    return on_message


def setup_logging(level_name: str) -> None:
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def main() -> int:
    options = load_options()
    setup_logging(options.get("log_level", "info"))

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="minew-mqtt-normalizer")
    username = str(options.get("mqtt_username") or "")
    password = str(options.get("mqtt_password") or "")
    if username:
        client.username_pw_set(username, password=password)

    runtime = Runtime(
        options=options,
        client=client,
        device_names=parse_overrides(options.get("device_name_overrides")),
        device_models=parse_overrides(options.get("device_model_overrides")),
    )
    client.on_connect = on_connect_factory(runtime)
    client.on_message = on_message_factory(runtime)

    def stop(signum: int, frame: Any) -> None:
        logging.info("Stopping due to signal %s", signum)
        runtime.running = False
        client.disconnect()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    logging.info("Starting Minew MQTT Normalizer")
    client.connect(str(options["mqtt_host"]), int(options["mqtt_port"]), keepalive=60)
    client.loop_start()

    try:
        while runtime.running:
            runtime.check_offline()
            time.sleep(5)
    finally:
        client.loop_stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
