import os, csv, argparse
from concurrent import futures
from helper import mapper, process_ca, empty_file, top_calculator, ca_by_store
from time import time

_SIZE = 1024*1024
_FILE_PATH = "/mnt/c/Users/nmado/Downloads/randomized-transactions-202009.psv/randomized-transactions-202009.psv"

def arg_parser():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--chunk_size", type=int, required=True, help="To choose the size of chunk we want to load from input data"
    )
    parser.add_argument(
        "--input_file", type=str, required=True, default=_FILE_PATH
    )
    args = parser.parse_args()
    return args.chunk_size, args.input_file

def main():

    chunk_size, input = arg_parser()
    print(chunk_size, input)
    
    

def process_top_50_products():

    ### loading input file by chunck
    ### multiplicateur = 300 ==> 6.71 minutes
    ### multiplicateur = 600 ==> 6.71 minutes
    mapper(_FILE_PATH, 1024 * _SIZE)

    # Create an empty file to be populated by SUM(CA)
    empty_file("first_iter_sum_ca.csv")

    chunks = [f for f in os.listdir("./chunck/")]
    # This is done in parallel using multiple threads.
    pool = futures.ThreadPoolExecutor(10)
    threads = []
    for chunk in chunks:
        threads.append(pool.submit(process_ca, f'./chunck/{chunk}', "first_iter_sum_ca.csv", 2, 5, 1))
    if len(threads) > 0:
        print("Waiting for all tasks to complete...")
        futures.wait(threads)
    # In order to capture error from any of the threads,
    for t in threads:
        _ = t.result()

    ### Second and last for Sum CA computing
    process_ca("first_iter_sum_ca.csv", "second_iter_sum_ca.csv", 0, 1, 2)

    ### Compute Top 50 products
    top_calculator("second_iter_sum_ca.csv", "Top_50_products.csv", 0, 1, 50)


def store_by_product(product):

    # Create an empty file to be populated by SUM(CA)
    empty_file(f"{product if product else _}_store_products.csv")

    chunks = [f for f in os.listdir("./chunck/")]
    # This is done in parallel using multiple threads.
    pool = futures.ThreadPoolExecutor(10)
    threads = []
    for chunk in chunks:
        threads.append(pool.submit(ca_by_store, product, f'./chunck/{chunk}', f"{product if product else _}_store_products.csv", 3, 5, 1))
    if len(threads) > 0:
        print("Waiting for all tasks to complete...")
        futures.wait(threads)
    # In order to capture error from any of the threads,
    for t in threads:
        _ = t.result()

    ### Second and last for Sum CA computing
    process_ca(f"{product if product else _}_store_products.csv", f"{product if product else _}_store_products_second_iter.csv", 0, 1, 2)

    ### Compute Top 100 store by top product
    top_calculator(f"{product if product else _}_store_products_second_iter.csv", f"top_100_products_store_{product if product else _}.csv", 0, 1, 100)

def process_top_100_store_by_product(filepath):
    prod_set = set()
    with open(filepath) as file:
        rows = csv.reader(file, delimiter="|")
        for row in rows:
            if row[0]:
                prod_set.add(f"{row[0]}") 
    for prod in prod_set:
        store_by_product(prod)

#debut = time() 
#process_top_50_products()
#process_top_100_store_by_product("Top_50_products.csv")
#fin = time()
#print(f"Execution duration: {(fin - debut)/60} Minutes")
if __name__ == "__main__":
    main()