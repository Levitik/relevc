""" The following modules should b tested for performance concern: Threading, Multiprocessing, Concurent"""

import os, csv, argparse
from concurrent import futures
from multiprocessing import Pool
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
        "--input_file", type=str, required=False, default=_FILE_PATH
    )
    args = parser.parse_args()
    return args.chunk_size, args.input_file

def main():

    ### Input options
    chunk_size, input = arg_parser()
    print(chunk_size, input)

    ### Start Processing Top 50 Products
    ### Thread based parallelism using concurent Module
    ### Test process based parallelism
    debut = time()
    process_top_50_products(chunk_size, input)
    fin = time()
    print(f"Top 50 CA duration: {(fin - debut)/60} Minutes")

    ### Start Processing Top 100 Store by Product
    ### Looping through each product and by using thread-based parallelism
    #process_top_100_store_by_product("Top_50_products.csv")
    debut = time()
    process_top_100_store("Top_50_products.csv")
    fin = time()
    print(f"Top 100 Store by product duration: {(fin - debut)/60} Minutes")

    
def process_top_50_products(chunk_size, inputpath):

    ### loading input file by chunck
    ### chunk_size = 300 ==> 6.71 minutes
    ### chunk_size = 600 ==> 6.71 minutes
    mapper(inputpath, chunk_size * _SIZE)

    # Create an empty file to be populated by SUM(CA)
    empty_file("first_iter_sum_ca.csv")

    chunks = [f for f in os.listdir("./chunk/")]
    # This is done in parallel using multiple threads.
    pool = futures.ThreadPoolExecutor(10)
    threads = []
    for chunk in chunks:
        threads.append(pool.submit(process_ca, f'./chunk/{chunk}', "first_iter_sum_ca.csv", 2, 5, 1))
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

    chunks = [f for f in os.listdir("./chunk/")]
    # This is done in parallel using multiple threads.
    pool = futures.ThreadPoolExecutor(10)
    threads = []
    for chunk in chunks:
        threads.append(pool.submit(ca_by_store, product, f'./chunk/{chunk}', f"{product if product else _}_store_products.csv", 3, 5, 1))
    if len(threads) > 0:
        print("Waiting for all tasks to complete...")
        futures.wait(threads)
    # In order to capture error from any of the threads,
    for t in threads:
        _ = t.result()

    ### Second and last for Sum CA computing
    process_ca(f"{product if product else _}_store_products.csv", f"{product if product else _}_store_products_second_iter.csv", 0, 1, 2)

    ### Compute Top 100 store by top product
    top_calculator(f"{product if product else _}_store_products_second_iter.csv", f"top_products_by_store/top_100_products_store_{product if product else _}.csv", 0, 1, 100)

def process_top_100_store_by_product(filepath):
    prod_set = set()
    with open(filepath) as file:
        rows = csv.reader(file, delimiter="|")
        for row in rows:
            if row[0]:
                prod_set.add(f"{row[0]}") 
    for prod in prod_set:
        print(f"Start Computation for {prod} product id")
        store_by_product(prod)


def process_top_100_store(filepath):
    prod_set = set()
    with open(filepath) as file:
        rows = csv.reader(file, delimiter="|")
        for row in rows:
            if row[0]:
                prod_set.add(f"{row[0]}") 
    with Pool(10) as pool:
        result = pool.map(store_by_product, prod_set)

#debut = time() 
#process_top_50_products()
#process_top_100_store_by_product("Top_50_products.csv")
#fin = time()
#print(f"Execution duration: {(fin - debut)/60} Minutes")
if __name__ == "__main__":
    main()