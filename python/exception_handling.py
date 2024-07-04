#Example 1:
try:
    n= int(input("Enter a number: "))
    result = 10 /n
    print(result)
except ZeroDivisionError:
    print("Error: Cannot divide by zero.")
except ValueError:
    print("Error: Invalid input. Please enter a valid number.")

#Example 2:

try:
    number = int(input("Enter a number: "))
    result = 10 / number
except ZeroDivisionError:
    print("Error: Cannot divide by zero.")
except ValueError:
    print("Error: Invalid input. Please enter a valid number.")
else:
    print(f"Result: {result}")
finally:
    print("Execution completed.")

#Example 3:

def check_age(age):
    if age < 18:
        raise ValueError("Age must be at least 18.")
    return "Access granted."

try:
    print(check_age(23))
except ValueError as e:
    print(f"Error: {e}")



