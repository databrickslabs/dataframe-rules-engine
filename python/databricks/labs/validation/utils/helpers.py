class Helpers:

    @staticmethod
    def to_java_array(py_array, sc):
        if type(py_array[0]) is int:
            java_type = sc._jvm.java.lang.Integer
            java_array = sc._gateway.new_array(java_type, len(py_array))
            for i in range(len(py_array)):
                java_array[i] = sc._jvm.java.lang.Integer.valueOf(py_array[i])
        else:
            java_type = sc._jvm.java.lang.String
            java_array = sc._gateway.new_array(java_type, len(py_array))
            for i in range(len(py_array)):
                java_array[i] = py_array[i]
        return java_array
