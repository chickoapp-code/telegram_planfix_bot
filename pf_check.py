import asyncio, json
from planfix_api import PlanfixAPIClient

async def main():
    c = PlanfixAPIClient()
    try:
        r = await c.get_contact_groups()
        print(json.dumps(r, ensure_ascii=False, indent=2))
        print("RESULT:", "OK" if r and r.get("result") == "success" else "FAIL")
    finally:
        await c.close()

asyncio.run(main())
