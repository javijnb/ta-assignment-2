import json

file_path = "output.json"
output_file_path = "parsed_output.json"

# Leer el archivo y eliminar la primera entrada "null"
with open(file_path, "r") as file:
    lines = file.readlines()[1:]

# Crear una lista de objetos JSON
data = []
for line in lines:
    line = line.strip()
    if line:
        data.append(json.loads(line.split("\t")[1]))

# Escribir el objeto JSON en un archivo
with open(output_file_path, "w") as output_file:
    json.dump(data, output_file, indent=4)

# Abre el archivo JSON
with open(output_file_path, 'r') as file:
    data = json.load(file)

# Ordena las entradas alfabéticamente
data_sorted = sorted(data, key=lambda x: list(x.keys())[0])

# Escribir el objeto JSON en un archivo
with open(output_file_path, "w") as output_file:
    json.dump(data_sorted, output_file, indent=4)