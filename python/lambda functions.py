# Example 1: Adding two numbers using a lambda function
add = lambda x, y: x + y
result = add(3, 5)
print("Result of addition:", result)

# Example 2: Filtering even numbers from a list using a lambda function with filter()
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
even_numbers = list(filter(lambda x: x % 2 == 0, numbers))
print("Even numbers:", even_numbers)

# Example 3: Doubling each element in a list using a lambda function with map()
numbers = [1, 2, 3, 4, 5]
doubled_numbers = list(map(lambda x: x * 2, numbers))
print("Doubled numbers:", doubled_numbers)
