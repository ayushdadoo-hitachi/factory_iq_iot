from datetime import datetime
import logging

def main():
    logging.basicConfig(level=logging.INFO)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Factory Hourly Refresh started at {ts}")
    print("Running factory hourly refresh tasks...")
    print("hello ayush3")
    logging.info("Factory Hourly Refresh completed.")

if __name__ == "__main__":
    main()