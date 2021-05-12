from faker.providers.person.en import Provider
import random
import math


# Return a randomized "range" using a Linear Congruential Generator
# to produce the number sequence. Parameters are the same as for
# python builtin "range".
#   Memory  -- storage for 8 integers, regardless of parameters.
#   Compute -- at most 2*"maximum" steps required to generate sequence.
#
def random_range(start, stop=None, step=None, seed=None):
    # Set a default values the same way "range" does.
    if stop is None:
        start, stop = 0, start
    if step is None:
        step = 1
    # Use a mapping to convert a standard range into the desired range.
    mapping = lambda i: (i*step) + start
    # Compute the number of numbers in this range.
    maximum = (stop - start) // step
    # Seed range with a random integer.
    if seed is not None:
        random.seed(seed)
    value = random.randint(0, maximum)
    #
    # Construct an offset, multiplier, and modulus for a linear
    # congruential generator. These generators are cyclic and
    # non-repeating when they maintain the properties:
    #
    #   1) "modulus" and "offset" are relatively prime.
    #   2) ["multiplier" - 1] is divisible by all prime factors of "modulus".
    #   3) ["multiplier" - 1] is divisible by 4 if "modulus" is divisible by 4.
    #
    offset = random.randint(0, maximum) * 2 + 1       # Pick a random odd-valued offset.
    multiplier = 4 * (maximum // 4) + 1               # Pick a multiplier 1 greater than a multiple of 4.
    modulus = int(2 ** math.ceil(math.log2(maximum))) # Pick a modulus just big enough to generate all numbers (power of 2).
    # Track how many random numbers have been returned.
    found = 0
    while found < maximum:
        # If this is a valid value, yield it in generator fashion.
        if value < maximum:
            found += 1
            yield mapping(value)
        # Calculate the next value in the sequence.
        value = (value * multiplier + offset) % modulus


def ip_address_generator(seed: int=None):
    """
    Generates ips with four randomint(1,256) joined by a dot. Can be improved by ensuring uniqueness for
    all 256 ** 4 outcomes in 256 ** 4 generations. First 100,000 are guaranteed unique.
    Args:
        seed: Any number to serve as a starting seed for random.seed

    Returns:
        ip address formatted xxx.xxx.xxx.xxx
    """
    if seed is not None:
        random.seed(seed)

    while True:
        yield '.'.join([random.randint(1, 256) for _ in range(4)])
        if seed is not None:
            seed += 1
            random.seed(seed)


def name_generator(first_names: list = None, last_names: list = None, repeats=False):
    if first_names is None:
        first_names = list(set(Provider.first_names))

    if last_names is None:
        last_names = list(set(Provider.last_names))

    # Making the names divide each other in length makes it simple to generate names without repeats
    if len(first_names) > len(last_names):
        remainder = len(first_names) % len(last_names)
        first_names = first_names[:-remainder]

    elif len(last_names) > len(first_names):
        remainder = len(last_names) % len(first_names)
        last_names = last_names[:-remainder]

    first_name_index = last_name_index = total_yielded = 0
    while repeats or total_yielded < len(first_names) * len(last_names):
        yield ' '.join([first_names[first_name_index], last_names[last_name_index]])

        total_yielded += 1
        last_name_index += 1
        first_name_index += 1

        if first_name_index == len(first_names):
            first_names.append(first_names.pop(0))
            first_name_index = 0
        if last_name_index == len(last_names):
            last_name_index = 0


if __name__ == '__main__':
    test = name_generator()
    for _ in range(100):
        print(next(test))