class CustomError(ValueError):
    def __str__(self):
        return "custom error message"

def raise_custom():
    raise CustomError()
