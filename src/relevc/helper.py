import csv

def file_writer(rows, file, prefix=None):
    filename = f"{file}_{prefix}.csv" if prefix else f"{file}.csv"
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f, delimiter="|")
        writer.writerows(rows)

def writer(rows, file, prefix=None):
    filename = f"./chunck/{file}_{prefix}.csv" if prefix else f"{file}.csv"
    with open(filename, 'w') as file:
        file.write(rows)

def mapper(path, size):
    with open(path, 'r') as file:
        while True:
            chunck = file.read(size)
            if not chunck:
                break
            writer(chunck, "randomize-transactions", file.tell()) 

def dev_mapper(path, size):
    read_from = 0
    with open(path, 'r') as file:
        while True:
            file.seek(read_from)
            chunck = file.read(size)
            if not chunck:
                break
            if len(chunck.splitlines()[-1].split("|")) != 6:
                read_from = file.tell() - len(chunck.splitlines()[-1])
            writer(chunck, "randomize-transactions", file.tell())
            
def process_ca(srcpath, destpath, k_index, v_index, iter):
    temp_dict = {}
    with open(srcpath) as file:
        rows = csv.reader(file, delimiter="|")
        try:
            for row in rows:
                if row[0] == "identifiant_client" or (len(row) != 6 if iter == 1 else len(row) != 2):
                    continue
                elif row[k_index] not in temp_dict:
                    temp_dict[row[k_index]] = round(float(row[v_index]),2)
                else:
                    temp_dict[row[k_index]] += round(float(row[v_index]),2)
        except ValueError as e:
            print(f'ValueError: Row value: {row} ==> filepath: {srcpath}')
        except IndexError as e:
            print(f'IndexError: Row value: {row} ==> filepath: {srcpath}')
    with open(destpath, 'a') as ca_file:
        writer = csv.writer(ca_file, delimiter="|")
        writer.writerows([[key, value] for key, value in temp_dict.items()])
        #writer = csv.DictWriter(ca_file, delimiter="|")
        #writer.writerows([[key, value] for key, value in temp_dict.items()])

def top_calculator(srcpath, destpath, k_index, v_index, top):
    temp_dict = {}
    with open(srcpath) as file:
        rows = csv.reader(file, delimiter="|")
        for row in rows:
            if row[k_index] not in temp_dict:
                temp_dict[row[k_index]] = round(float(row[v_index]),2)
            else:
                temp_dict[row[k_index]] += round(float(row[v_index]),2)
        sorted_dict = dict(sorted(temp_dict.items(), key = lambda x: x[1], reverse = True)[:top])
    with open(destpath, 'a') as ca_file:
        writer = csv.writer(ca_file, delimiter="|")
        writer.writerows([[key, value] for key, value in sorted_dict.items()])

def empty_file(filepath):
    with open(filepath, 'w') as f:
        pass

def ca_by_store(product, srcpath, destpath, k_index, v_index, iter):
    temp_dict = {}
    with open(srcpath) as file:
        rows = csv.reader(file, delimiter="|")
        try:
            for row in rows:
                if row[2]  == product:
                    if row[0] == "identifiant_client" or (len(row) != 6 if iter == 1 else len(row) != 2):
                        continue
                    elif row[k_index] not in temp_dict:
                        temp_dict[row[k_index]] = round(float(row[v_index]),2)
                    else:
                        temp_dict[row[k_index]] += round(float(row[v_index]),2)
                else:
                    continue
        except ValueError as e:
            print(f'ValueError: Row value: {row} ==> filepath: {srcpath}')
        except IndexError as e:
            print(f'IndexError: Row value: {row} ==> filepath: {srcpath}')
    with open(destpath, 'a') as ca_file:
        writer = csv.writer(ca_file, delimiter="|")
        writer.writerows([[key, value] for key, value in temp_dict.items()])


#empty_file()
#process_ca("chunck/randomize-transactions_7864320000.csv", "first_iter_sum_ca.csv", 2, 5, 1)
### Second and last for Sum CA computing
#process_ca("first_iter_sum_ca.csv", "second_iter_sum_ca.csv", 0, 1, 2)
