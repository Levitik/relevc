import os
import time
import multiprocessing as mp
import csv

# Top 100 Product By Store
# Top Store By Product

_SIZE = mp.cpu_count()
_FILE_PATH = "/mnt/c/Users/nmado/Downloads/randomized-transactions-202009.psv/randomized-transactions-202009.psv"

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

def _process_ca(file_name: str, start: int, end: int):
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

def _process_prod(file_name: str, start: int, end: int):
    # rsults = {id_prod: {id_mag: ca}}
    rsults = {}
    with open(file_name, "r") as file:
        file.seek(start)
        for line in file:
            start += len(line)
            if start > end:
                break
            _, _, id_prod, id_mag, _, price = line.split("|")
            if rsults.get(id_prod):
                #rsults[id_prod] += float(price)
                if rsults.get(id_prod).get(id_mag):
                    rsults[id_prod][id_mag] += float(price)
                else:
                    rsults[id_prod][id_mag] = float(price)
            else:
                rsults[id_prod] = {id_mag: float(price)}
    return rsults

#0.97
def top_100_ca(cpu: int, chunks: list):
    all_ca = {}
    with mp.Pool(cpu) as p:
        chunk_rsults = p.starmap(_process_ca, chunks)
        for chunk_rsult in chunk_rsults:
            for id_prod, price in chunk_rsult.items():
                if all_ca.get(id_prod):
                    all_ca[id_prod] += float(price)
                else:
                    all_ca[id_prod] = float(price)
        all_ca_sorted = sorted(all_ca.items(), key=lambda item: item[1], reverse=True)
        file_writer("top_100_produit.csv", all_ca_sorted[:100])
    return all_ca_sorted[:50]

def file_writer(file_name, data):
    with open(file_name, 'w') as file:
        writer = csv.writer(file, delimiter="|")
        writer.writerows([[key, value] for key, value in data])

def main(chunks):

    with mp.Pool(8) as p:
        all_ca = {}
        chunk_rsults = p.starmap(_process_ca, chunks)
        for chunk_rsult in chunk_rsults:
            for id_prod, price in chunk_rsult.items():
                if all_ca.get(id_prod):
                    all_ca[id_prod] += float(price)
                else:
                    all_ca[id_prod] = float(price)
        all_ca_sorted = sorted(all_ca.items(), key=lambda item: item[1], reverse=True)
        file_writer("top_50_produit.csv", all_ca_sorted[:50])

    with mp.Pool(8) as p:
        all_prod = {}
        chunk_rsults = p.starmap(_process_prod, chunks)
        for chunk_rsult in chunk_rsults:
            for id_prod, mag_price in chunk_rsult.items():
                if all_prod.get(id_prod):
                    for id_mag, price in mag_price.items():
                        if all_prod.get(id_prod).get(id_mag):
                            all_prod[id_prod][id_mag] += float(price)
                        else:
                            all_prod[id_prod][id_mag] = float(price)
                else:
                    all_prod[id_prod] = mag_price
        for id_prod in all_prod:
            all_prod_sorted = sorted(all_prod[id_prod].items(), key=lambda item: item[1], reverse=True)
            file_writer(f"/home/nmadogni/interview/relevc/store/top_100_products_store_{id_prod}.csv", all_prod_sorted[:100])

def test(chunks):
    with mp.Pool(8) as p:
        all_prod = {}
        chunk_rsults = p.starmap(_process_prod, chunks)
        for chunk_rsult in chunk_rsults:
            for id_prod, mag_price in chunk_rsult.items():
                if all_prod.get(id_prod):
                    for id_mag, price in mag_price.items():
                        if all_prod.get(id_prod).get(id_mag):
                            all_prod[id_prod][id_mag] += float(price)
                        else:
                            all_prod[id_prod][id_mag] = float(price)
                else:
                    all_prod[id_prod] = mag_price
        for id_prod in all_prod:
            all_prod_sorted = sorted(all_prod[id_prod].items(), key=lambda item: item[1], reverse=True)
            file_writer(f"/home/nmadogni/interview/relevc/store/top_100_products_store_{id_prod}.csv", all_prod_sorted[:100])


if __name__ == "__main__":
    
    start_time = time.time()
    top_100_ca(_SIZE, file_chunk(_FILE_PATH, _SIZE))
    #main(file_chunk(_FILE_PATH, _SIZE))
    print(f"End time: {(time.time() - start_time)/60}")