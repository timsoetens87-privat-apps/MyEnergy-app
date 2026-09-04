import argparse
import json
import requests

DEFAULT_HOST = "192.168.0.27"
DEFAULT_TIMEOUT_SECONDS = 3.0


def endpoint_candidates(host, explicit_url=None):
	if explicit_url:
		return [explicit_url]
	return [
		f"http://{host}/api/v1/status",
		f"http://{host}/api/status",
		f"http://{host}/status",
		f"http://{host}/api/battery/status",
		f"http://{host}/api/device/status",
		f"http://{host}/",
	]


def read_status(host, timeout_seconds, url=None, token=None):
	headers = {"Accept": "application/json"}
	if token:
		headers["Authorization"] = f"Bearer {token}"

	last_error = None
	for endpoint in endpoint_candidates(host, explicit_url=url):
		try:
			response = requests.get(endpoint, headers=headers, timeout=float(timeout_seconds))
			response.raise_for_status()
			return {
				"source": endpoint,
				"payload": response.json(),
			}
		except Exception as exc:
			last_error = f"{endpoint}: {exc}"

	raise RuntimeError(last_error or "Unable to read Marstek HTTP status")


def parse_args():
	parser = argparse.ArgumentParser(description="Read current Marstek status over HTTP")
	parser.add_argument("--host", default=DEFAULT_HOST, help="Marstek IP address")
	parser.add_argument("--url", default="", help="Explicit Marstek status URL")
	parser.add_argument("--token", default="", help="Optional bearer token")
	parser.add_argument(
		"--timeout",
		type=float,
		default=DEFAULT_TIMEOUT_SECONDS,
		help="HTTP request timeout in seconds",
	)
	return parser.parse_args()


def main():
	args = parse_args()
	result = read_status(args.host, args.timeout, url=args.url or None, token=args.token or None)
	print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
	main()