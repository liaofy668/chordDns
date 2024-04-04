import random
import string

def generate_strings():
    strings = set()
    while len(strings) < 5000:
        string1 = ''.join(random.choices(string.ascii_letters, k=8))
        string2 = string1
        strings.add(f"{string1} {string2}")
    return list(strings)

def write_to_file(strings):
    with open('input.txt', 'w') as file:
        for string in strings:
            file.write(string + '\n')

strings = generate_strings()
write_to_file(strings)


# import random

def select_strings():
    with open('input.txt', 'r') as file:
        strings = file.readlines()
    selected_strings = []
    while len(selected_strings) < 5000:
        string_pair = random.choice(strings)
        string1, string2 = string_pair.split()
        selected_strings.append(string1)
    with open('query.txt', 'w') as file:
        for string in selected_strings:
            file.write(string + '\n')

select_strings()


