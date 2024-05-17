class Rectangle:
    def __init__(self, length, width):
        self.length = length
        self.width = width

    def calculate_area(self):
        return self.length * self.width

    def calculate_perimeter(self):
        return 2 * (self.length + self.width)

# Test the class
rectangle = Rectangle(5, 3)
print("Area:", rectangle.calculate_area())
print("Perimeter:", rectangle.calculate_perimeter())
