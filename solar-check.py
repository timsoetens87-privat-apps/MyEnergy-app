import time
from pymodbus.client import ModbusTcpClient

HOST = "192.168.0.28"
PORT = 502
TIMEOUT_SECONDS = 10
POLL_INTERVAL_SECONDS = 2
DEVICE_ID_CANDIDATES = [1]


def current_timestamp():
	return time.strftime("%Y-%m-%d %H:%M:%S")


def read_holding_with_compat(client, address, count, device_id):
	read_func = client.read_holding_registers
	try:
		return read_func(address=address, count=count, unit=device_id)
	except TypeError:
		try:
			return read_func(address=address, count=count, slave=device_id)
		except TypeError:
			try:
				return read_func(address=address, count=count, device_id=device_id)
			except TypeError:
				try:
					return read_func(address, count, unit=device_id)
				except TypeError:
					try:
						return read_func(address, count, slave=device_id)
					except TypeError:
						return read_func(address, count, device_id=device_id)


def decode_i32(registers):
	raw = (int(registers[0]) << 16) | int(registers[1])
	if raw & 0x80000000:
		raw -= 0x100000000
	return raw


def read_active_power_w(client):
	errors = []
	for device_id in DEVICE_ID_CANDIDATES:
		try:
			result = read_holding_with_compat(client, address=32080, count=2, device_id=device_id)
			if not result.isError() and len(getattr(result, "registers", []) or []) >= 2:
				return decode_i32(result.registers), None
			errors.append(f"device_id={device_id} response={result}")
		except Exception as exc:
			errors.append(f"device_id={device_id} error={exc}")
	return None, "; ".join(errors)


def main():
	client = ModbusTcpClient(HOST, port=PORT, timeout=TIMEOUT_SECONDS)
	try:
		while True:
			if not client.connect():
				print(f"{current_timestamp()} active_power_w=unavailable", flush=True)
				time.sleep(POLL_INTERVAL_SECONDS)
				continue

			try:
				active_power_w, last_error = read_active_power_w(client)
			except Exception:
				active_power_w = None
				last_error = "unexpected read exception"

			if active_power_w is None:
				if last_error:
					print(f"{current_timestamp()} active_power_w=unavailable ({last_error})", flush=True)
				else:
					print(f"{current_timestamp()} active_power_w=unavailable", flush=True)
			else:
				print(f"{current_timestamp()} active_power_w={active_power_w}", flush=True)

			time.sleep(POLL_INTERVAL_SECONDS)
	finally:
		try:
			client.close()
		except Exception:
			pass


if __name__ == "__main__":
	main()
