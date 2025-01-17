import csv
import time

# Top 50 product: based on CA
# top 100 store per top 50 products: les 100 magasins pour chacun des 50 meilleurs products
def file_writer(file_name, data):
    with open(file_name, 'w') as file:
        writer = csv.writer(file, delimiter="|")
        writer.writerows([[key, value] for key, value in data])

# 1.85 minute
def top_n_ca(input_file, output_file, top_n):
    rsults = {}
    with open(input_file, "r") as file:
        next(file)
        for line in file:
            _, _, id_prod, _, _, price = line.split("|")
            if rsults.get(id_prod):
                rsults[id_prod] += float(price)
            else:
                rsults[id_prod] = float(price)
        rsults_sorted = sorted(rsults.items(), key=lambda item: item[1], reverse=True)[:top_n]
        file_writer(output_file, rsults_sorted)
        return dict(rsults_sorted[1:])

# Top N store by top N product        
def top_store_by_top_prd(input_file, top_n, filters):
    all_set = {} #{id_prod: {id_mag: ca}}
    with open(input_file, "r") as file:
        next(file)
        for line in file:
            _, _, id_prod, id_mag, _, price = line.split("|")
            if id_prod in filters:
                if all_set.get(id_prod):
                    if all_set.get(id_prod).get(id_mag):
                        all_set[id_prod][id_mag] += float(price)
                    else:
                        all_set[id_prod][id_mag] = float(price)
                else:
                    all_set[id_prod] = {id_mag: float(price)}
        for id_prod in all_set:
            all_set_sorted = sorted(all_set[id_prod].items(), key=lambda item: item[1], reverse=True)[:top_n]
            file_writer(f"/home/nmadogni/interview/relevc/store/top_50_products_store_{id_prod}.csv", all_set_sorted)


# Top N store by product        
def top_store_by_prd(input_file, top_n):
    all_set = {} #{id_prod: {id_mag: ca}}
    with open(input_file, "r") as file:
        next(file)
        for line in file:
            _, _, id_prod, id_mag, _, price = line.split("|")
            if all_set.get(id_prod):
                if all_set.get(id_prod).get(id_mag):
                    all_set[id_prod][id_mag] += float(price)
                else:
                    all_set[id_prod][id_mag] = float(price)
            else:
                all_set[id_prod] = {id_mag: float(price)}
        for id_prod in all_set:
            all_set_sorted = sorted(all_set[id_prod].items(), key=lambda item: item[1], reverse=True)[:top_n]
            file_writer(f"/home/nmadogni/interview/relevc/store/top_store_{id_prod}.csv", all_set_sorted)


if __name__ == "__main__":
    start = time.time()
    input_f = "/mnt/c/Users/nmado/Downloads/randomized-transactions-202009.psv/randomized-transactions-202009.psv"
    output_f = "origin_top_ca.csv"
    filters = top_n_ca(input_f, output_f, 50)
    #top_store_by_top_prd(input_f, 100, filters)
    top_store_by_prd(input_f, 100)
    print(f"Processing time: {(time.time() - start)/60}")
    






