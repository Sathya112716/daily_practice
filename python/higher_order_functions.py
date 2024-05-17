def add(x, y):
    return x + y

def subtract(x, y):
    return x - y

def multiply(x, y):
    return x * y

def apply_operation(operation, x, y):
    return operation(x, y)

# Test the functions
print(apply_operation(add, 5, 3))
print(apply_operation(subtract, 10, 4))
print(apply_operation(multiply, 2, 7))
