import aiohttp
import asyncio
import time
from statistics import mean

URLS = {
    "Lighter": "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails",
    "Extended": "https://api.starknet.extended.exchange/api/v1/info/markets?market=BTC-USD",
}

SAMPLES = 5  # number of requests per API


async def measure_latency(session, name, url):
    latencies = []
    for i in range(SAMPLES):
        start = time.perf_counter()
        try:
            async with session.get(url) as resp:
                await resp.text()  # read response to complete the request
                elapsed = (time.perf_counter() - start) * 1000
                latencies.append(elapsed)
                print(f"[{name}] Run {i+1}/{SAMPLES}: {elapsed:.2f} ms (status {resp.status})")
        except Exception as e:
            print(f"[{name}] Error on run {i+1}: {e}")
            latencies.append(None)
        await asyncio.sleep(1)  # small delay between requests
    return [x for x in latencies if x is not None]


async def main():
    async with aiohttp.ClientSession() as session:
        results = {}
        for name, url in URLS.items():
            print(f"\n--- Testing {name} ---")
            latencies = await measure_latency(session, name, url)
            results[name] = latencies

        print("\n===== SUMMARY =====")
        for name, latencies in results.items():
            if latencies:
                print(
                    f"{name}: avg {mean(latencies):.2f} ms, min {min(latencies):.2f}, max {max(latencies):.2f}"
                )
            else:
                print(f"{name}: all tests failed")


if __name__ == "__main__":
    asyncio.run(main())
