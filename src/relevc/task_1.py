# It has always been the case that Python’s multi-threaded performance has never lived up to expectations because of GIL.
# So since version 3.4, Python has introduced the asyncio package to execute IO-bound tasks through concurrency concurrently.
# Must do this tutorial: https://www.dataleadsfuture.com/use-these-methods-to-make-your-python-concurrent-tasks-perform-better/

import time
import multiprocessing as mp
import os
import csv



_SIZE = mp.cpu_count()
_FILE_PATH = "/mnt/c/Users/nmado/Downloads/randomized-transactions-202009.psv/randomized-transactions-202009.psv"

def file_writer(file_name, data):
    with open(file_name, 'w') as file:
        writer = csv.writer(file, delimiter="|")
        writer.writerows([[key, value] for key, value in data])

def file_chunk(file_name: str, cpu_cores: int):
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // cpu_cores
    chunks = []
    with open(file_name, "r") as file:
        # Omit the fisrt line: 93
        chunk_start = 94 
        while chunk_start < file_size:
            chunk_end = min(file_size, chunk_start + chunk_size)
            #Move file pointer to specific location in the file
            file.seek(chunk_end)
            #Make sure we find a line break
            file.readline()
            #Find the curret location of the file pointer
            chunk_end = file.tell()
            chunks.append((file_name, chunk_start, chunk_end))
            chunk_start = chunk_end
    return chunks

def prod_mapper(file_name: str, start: int, end: int):
    rsults = {}
    with open(file_name, "r") as file:
        file.seek(start)
        for line in file:
            start += len(line)
            if start > end:
                break
            _, _, id_prod, _, _, price = line.split("|")
            if rsults.get(id_prod):
                rsults[id_prod] += float(price)
            else:
                rsults[id_prod] = float(price)
    return rsults

def store_mapper(file_name: str, start: int, end: int):
    top_mag = {}
    with open(file_name, "r") as file:
        file.seek(start)
        for line in file:
            start += len(line)
            if start > end:
                break
            _, _, id_prod, id_mag, _, price = line.split("|")
            if top_mag.get(id_prod):
                if top_mag.get(id_prod).get(id_mag):
                    top_mag[id_prod][id_mag] += float(price)
                else:
                    top_mag[id_prod][id_mag] = float(price)
            else:
                top_mag[id_prod] = {id_mag: float(price)}
    return top_mag

def map_all(file_name: str, start: int, end: int):
    top_prod, top_mag = {}, {}
    with open(file_name, "r") as file:
        file.seek(start)
        for line in file:
            start += len(line)
            if start > end:
                break
            _, _, id_prod, id_mag, _, price = line.split("|")
            # for Top prdocut by CA
            if top_prod.get(id_prod):
                top_prod[id_prod] += float(price)
            else:
                top_prod[id_prod] = float(price)
            # for Top store per all product
            if top_mag.get(id_prod):
                if top_mag.get(id_prod).get(id_mag):
                    top_mag[id_prod][id_mag] += float(price)
                else:
                    top_mag[id_prod][id_mag] = float(price)
            else:
                top_mag[id_prod] = {id_mag: float(price)}
    return [top_prod, top_mag]

def apply_prod_mapper(cpu: int, chunks: list):
    with mp.Pool(cpu) as p:
        chunk_rsults = p.starmap(prod_mapper, chunks)
    return chunk_rsults  

def apply_store_mapper(cpu: int, chunks: list):
    with mp.Pool(cpu) as p:
        store_rsults = p.starmap(store_mapper, chunks)
    return store_rsults
    

def reducer(resultats: list[dict[str, float]]):
    all_ca = {}
    for resultat in resultats:
        for id_prod, price in resultat.items():
            if all_ca.get(id_prod):
                all_ca[id_prod] += float(price)
            else:
                all_ca[id_prod] = float(price)
    all_ca_sorted = sorted(all_ca.items(), key=lambda item: item[1], reverse=True)
    file_writer("top_50_produit.csv", all_ca_sorted[:50])

#def reduce_all(all_set: list[list[dict[str, float],dict[str, dict[str, float]]]]):


if __name__ == "__main__":
    start = time.time()
    #reducer(apply_mapper(_SIZE, file_chunk(_FILE_PATH, _SIZE)))
    apply_store_mapper(_SIZE, file_chunk(_FILE_PATH, _SIZE))
    #apply_store_mapper(_SIZE, file_chunk(_FILE_PATH, _SIZE))
    print(f"Time for Completion: {(time.time() - start)/60}")
    