import random

SEED = 1234


def initialize_random_seed() -> None:
    """Set random seed to a fixed value to make sure any forecast step is reproducible."""
    random.seed(SEED)
