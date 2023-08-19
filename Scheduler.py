import time
import subprocess
from datetime import datetime, timedelta


def run_scraper():
    try:
        # Run the scraper using subprocess
        result = subprocess.run(
            ['python', 'zameen_scrapper.py'], capture_output=True, text=True)
        print(result.stdout)
    except Exception as e:
        print(str(e))


def main():
    while True:
        now = datetime.now()
        target_time = now.replace(
            hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

        # Calculate the time remaining until 12 am
        time_remaining = (target_time - now).total_seconds()
        if time_remaining == 0:
            print("Running scraper...")
            run_scraper()

        else:
            continue
        # Sleep until 12 am
        # print(f"Waiting for {time_remaining} seconds until 12 am...")
        # time.sleep(time_remaining)

        # Run the scraper at 12 am


if __name__ == '__main__':
    main()
