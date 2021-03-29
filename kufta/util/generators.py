def is_prime(n):
    # Corner cases
    if n <= 1:
        return False
    if n <= 3:
        return True

    # This is checked so that we can skip the middle five numbers in the loop below
    if n % 2 == 0 or n % 3 == 0:
        return False

    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i = i + 6

    return True


def nearest_prime(num: int, round_down=False):
    if type(num) is not int:
        Warning("Provided non integer. Converting...")
        num = int(num)

    while not is_prime(num):
        if round_down:
            num -= 1
        else:
            num += 1
    return num


def number_generator(upper_bound: int,
                     lower_bound: int = 0,
                     shift_amount: int = 0,
                     repeats: bool = False):
    """
    Generates all unique numbers between lower_bound and upper_bound exactly once in a somewhat random way.
    Args:
        upper_bound: Highest value to generate (rounded down to the nearest prime)
        lower_bound: Lowest value to generate
        shift_amount: How far to shift the values up (i.e. shift 1e10 for phone number to all have length 10)
        repeats: Whether or not to keep generating numbers after all numbers in the range have been exhausted

    Returns:
        Returns a generator for generating numbers as message come in.
    """
    prime_upper_bound = nearest_prime(upper_bound)
    # Put the first number roughly in the middle to look more random
    current = seed_number = nearest_prime(lower_bound + (upper_bound - lower_bound) * 7 // 17)
    counter = 0
    # Checks if all the numbers have been generated between lower and upper, then checks if repeats are allowed
    while counter < upper_bound - lower_bound or repeats:
        while current < lower_bound or current > upper_bound:
            current = current + seed_number % prime_upper_bound
        yield current + shift_amount
        current = current + seed_number % prime_upper_bound
        counter += 1


def name_generator(first_names: list, last_names: list, repeats=False):
    # Making the names divide each other in length makes it simple to generate names without repeats
    if len(first_names) > len(last_names):
        remainder = len(first_names) % len(last_names)
        first_names = first_names[:-remainder]

    elif len(last_names) > len(first_names):
        remainder = len(last_names) % len(first_names)
        last_names = last_names[:-remainder]

    first_name_index = last_name_index = total_yielded = 0
    while repeats or total_yielded == len(first_names) * len(last_names):
        if first_name_index == len(first_names):
            first_names.insert(0, first_names.pop())
            first_name_index = 0
        if last_name_index == len(last_names):
            last_name_index = 0

        total_yielded += 1
        last_name_index += 1
        first_name_index += 1

        yield ' '.join([first_names[first_name_index], last_names[last_name_index]])


# class NumberMorph(object):
#     def __init__(self, upper_bound: int, lower_bound: int = 0, add_on_val=0, repeats=False):
#         self.generator = number_generator(upper_bound, lower_bound, add_on_val, repeats)
#
