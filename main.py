from config import logger, settings
from core.lseg import LSEG
from database import MSSQLDatabase
from transformer import Agent


def main():
    logger.info("Initializing Scraper Engine")
    client = LSEG()
    result = client.run()
    transformer = Agent(result)
    logger.info("Transforming Data")
    df_transformed = transformer.transform()
    logger.info(f"\n{df_transformed}")
    conn = MSSQLDatabase()
    logger.info(f"Inserting Data into {settings.OUTPUT_TABLE}")
    conn.insert_table(df_transformed, settings.OUTPUT_TABLE)
    logger.info("Application completed successfully")
    return


if __name__ == "__main__":
    main()
