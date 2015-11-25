# Metodos para normalizar los archivos del DataSet

def items (input):
    file = open(input)
    data = {}
    for line in file:
        items_info = line.split('|')
        data[int(items_info[0])] = items_info[1]

    return data

def normalizar (input, item_input, output):
    file = open(input)
    outputFile = open(output,'w')

    info_items = items(item_input)
    for line in file:
        user_id, item_id, rating, timestamp = line.split('\t')
        outputFile.write('|'.join([user_id, info_items[int(item_id)], rating]) + '\n')

    outputFile.close()
    file.close()

normalizar('u.data', 'u.item', 'ratings.csv')