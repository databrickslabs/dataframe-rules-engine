class Helpers:

    @staticmethod
    def to_java_array(py_array, sc):
        if type(py_array[0]) == 'str':
            java_type = sc._jvm.java.lang.String
        elif type(py_array[0]) == 'int':
            java_type = sc._jvm.java.lang.Integer
        elif type(py_array[0]) == 'long':
            java_type = sc._jvm.java.lang.Long
        elif type(py_array[0]) == 'float':
            java_type = sc._jvm.java.lang.Double
        else:
            java_type = sc._jvm.java.lang.String
        java_array = sc._gateway.new_array(java_type, len(py_array))
        for i in range(len(py_array)):
            java_array[i] = py_array[i]
        return java_array