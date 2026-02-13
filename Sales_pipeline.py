import logging
import sys
def setup_logging():
    logger=logging.getLogger(__name__)
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    
    c_handler=logging.StreamHandler(sys.stdout)
    c_handler.setLevel(logging.WARNING)
    f_handler= logging.FileHandler("sales.log")
    f_handler.setLevel(logging.DEBUG)
    
    simple=logging.Formatter("%(levelname)s-%(message)s")
    detail=logging.Formatter("%(asctime)s-%(levelname)s-%(message)s")
    
    c_handler.setFormatter(simple)
    f_handler.setFormatter(detail)
    
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    logger.debug("Started the Program")
    return logger
def read_sales_data(filename, logger):
    items={}
    try:
        with open(filename,"r")as file:
            logger.info("opening the File")
            for line in file:
                content=line.split(',')
                try:
                    name=content[0].strip()
                    items[name]=items.get(name,0)+int(content[1].strip())
                    logger.debug(f"Processing {line}: product:{name.strip()},Quantity:{items[name]}")
                except ValueError as e:
                    logger.warning(f"Not correct data format at {line}")
                    continue
                except IndexError as e:
                    logger.warning(f"not sufficient data at {line}")
                    continue
            return items
    except FileNotFoundError as e:
        logger.error("file is not found")
        return None
def write_sales_totals(data, output_file, logger):
    try:
        with open(output_file,"w")as output:
            logger.info("Writting total_Sales File !")
            output.write('product,quantity\n')
            for key,value in data.items():
                output.write(f"{key},{value}\n")
    except IOError as e :
        logger.error(f"Failed to Write the Output File :{e}")
        raise


def main():
    """Main pipeline orchestration."""
    logger = setup_logging()
    
    try:
        logger.info("Starting sales pipeline")
        
        sales_data = read_sales_data("Sales.txt", logger)
        if sales_data is not None:
            write_sales_totals(sales_data, "total_sales.txt", logger)
            logger.info(f"Pipeline complete - {len(sales_data)} products")
        else:
            logger.error("Pipeline failed - input file missing")
            
    except Exception as e:
        logger.critical(f"Unexpected error in pipeline: {e}")
        raise

if __name__ == "__main__":
    main()