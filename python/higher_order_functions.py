def add(x, y):#Define the function:
    return x + y

def subtract(x, y):
    return x - y

def multiply(x, y):
    return x * y

def apply_operation(operation, x, y):
    return operation(x, y)

# Test the functions:
print(apply_operation(add, 5, 3))
print(apply_operation(subtract, 10, 4))
print(apply_operation(multiply, 2, 7))

# Example using map and filter

def square(x):
    return x * x

def is_even(x):
    return x % 2 == 0

numbers = [1, 2, 3, 4, 5]

squared_numbers = list(map(square, numbers))
print(squared_numbers)

even_numbers = list(filter(is_even, numbers))
print(even_numbers)

# Example of a closure

def make_multiplier_of(n):
    def multiplier(x):
        return x * n
    return multiplier

times3 = make_multiplier_of(3)

times5 = make_multiplier_of(5)

print(times3(9))
print(times5(3))
print(times5(times3(2)))

from functools import reduce

# Example using reduce

def multiply(x, y):
    return x * y

numbers = [1, 2, 3, 4, 5]

result = reduce(multiply, numbers)
print(result)

# Example of a simple decorator

def my_decorator(func):
    def wrapper():
        print("Something is happening before the function is called.")
        func()
        print("Something is happening after the function is called.")
    return wrapper

@my_decorator
def say_hello():
    print("Hello!")

say_hello()



