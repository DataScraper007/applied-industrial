import subprocess
import math
import multiprocessing
import time

def run_browser_command(port, start_id, end_id, num_tabs):
    """Function to run the browser command in a separate process."""
    command = f"python mbmt.py {port} {start_id} {end_id} {num_tabs}"
    print(f"Running: {command}")
    
    start_time = time.time()
    
    subprocess.Popen(command, shell=True).wait() 
    
    end_time = time.time()

    elapsed_time = end_time - start_time
    print(f"Process for port {port} (ID range {start_id}-{end_id}) took {elapsed_time:.2f} seconds.")

def main():
    try:
        num_browsers = int(input("Enter the number of browsers: "))
        num_tabs = int(input("Enter the number of tabs per browser: "))
        total_records = int(input("Enter the total number of records: "))
    except ValueError:
        print("Please enter valid integers for the inputs.")
        return

    if num_browsers <= 0 or num_tabs <= 0 or total_records <= 0:
        print("All input values must be greater than zero.")
        return

    records_per_browser = math.ceil(total_records / num_browsers)

    base_port = 9221

    processes = []

    for i in range(num_browsers):
        start_id = i * records_per_browser + 1
        end_id = min((i + 1) * records_per_browser, total_records)

        port = base_port + i

        process = multiprocessing.Process(target=run_browser_command, args=(port, start_id, end_id, num_tabs))
        processes.append(process)

        process.start()

    for process in processes:
        process.join()

    print("All browser commands have finished execution.")

if __name__ == "__main__":
    main()
