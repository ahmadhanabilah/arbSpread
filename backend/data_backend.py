import asyncio
import logging
from db_lig.main import processDbLig
from db_ext.main import processDbExt

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_backend")

async def main():
    task1 = asyncio.create_task(processDbExt())
    task2 = asyncio.create_task(processDbLig())
    await asyncio.gather(task1, task2)
    
if __name__ == "__main__":
    asyncio.run(main())
