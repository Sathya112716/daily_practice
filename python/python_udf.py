#Function to Calculate the Area of a Circle
import math

def area_of_circle(radius):
    return math.pi * (radius ** 2)

print(area_of_circle(5))

#Function to Check if a Number is Prime
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            return False
    return True

print(is_prime(11))
print(is_prime(4))

#Function to Find the Factorial of a Number
def factorial(n):
    if n == 0:
        return 1
    else:
        return n * factorial(n - 1)

print(factorial(5))

#Function to Find the Fibonacci Sequence Up to n Terms
def fibonacci(n):
    fib_sequence = [0, 1]
    while len(fib_sequence) < n:
        fib_sequence.append(fib_sequence[-1] + fib_sequence[-2])
    return fib_sequence[:n]

print(fibonacci(10))

#Function to Reverse a String
def reverse_string(s):
    return s[::-1]

# Usage
print(reverse_string("hello"))
