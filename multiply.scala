import org.apache.spark.sql.Row

val dataM = spark.read.format("csv").option("header", "false").option("delimiter", ",").load("/home/mqp/Desktop/Matrix1.csv")

val dataN = spark.read.format("csv").option("header", "false").option("delimiter", ",").load("/home/mqp/Desktop/Matrix2.csv")

val mapM = dataM.map(x => (x.getString(1), ("M", x.getString(0), x.getString(2).toString)))
// map based on j

val mapN = dataN.map(x => (x.getString(0), ("N", x.getString(1), x.getString(2).toString)))
// map based on i

val product = mapM.join(mapN, "_1").map({ case Row(j: String, Row(f1: String, i: String, m: String), Row(f2: String, k: String, n: String)) => (i, k, m.toDouble * n.toDouble) }).toDF("i", "k", "v")
// join by key calculate the product, use double to avoid overflow

product.groupBy("i", "k").sum("v").toDF("i", "k", "Tik").show
// group by keys and output the result

