import java.io.File

import kantan.csv._
import kantan.csv.ops._
//Add for class case use
import kantan.csv.generic._

import scala.collection.immutable.ListMap
import scala.io.Codec

implicit val codec:Codec = Codec.ISO8859

case class Matricula(
                      provincia: String,
                      clase: String,
                      combustible: String,
                      marca: String,
                      servicio: String,
                      modelo: String,
                      tonelaje: Double,
                      asientos: Int,
                      estratone: String,
                      estrasientos: String
                    )

val path2DataFile = "C:\\2008vehiculos.csv"

val dataSource = new File(path2DataFile)
  .readCsv[List, Matricula](rfc.withHeader())


val rows = dataSource.filter(row => row.isRight)
println(rows.size)
/*val errors = dataSource.filter(row => row.isLeft)
printf("Errors: %d", errors.size)*/

//val values = rows.collect({ case Right(t10) => t10 })
val values = rows.collect({ case Right(matricula) => matricula })
//values.take(15).foreach(println)

//¿Cuántas y cuáles son las clases de vehículos que existen en los registros?
//val clasesV = values.map(_._2).distinct.sorted
/*
val clasesV = values.map(_.clase).distinct.sorted
clasesV.foreach(println)
*/
//¿Cuántas motocicletas tiene el estado y cuáles son sus marcas, clasificadas por provincia?
//val motocicletas = values
 // .filter(row =>
 //   row.servicio == "ESTADO" && row.clase == "Motocicleta")
 // .groupBy(row => (row.provincia, row.marca))
 // .map({ case ((prov, marca), v) => (prov, marca, v.length)})
//motocicletas.foreach(data => printf("%s, %s, %d\n", data._1, data._2, data._3))


//¿Cuáles son las marcas, clases y el servicio que prestan los vehículos que usan Gas liquado de petróleo?
val dataGLP = values
  .filter(row => row.combustible.startsWith("Gas liq"))
  .map(row=> (row.marca, row.clase, row.servicio))
  .groupBy(identity)
  .map({ case((marca, clase, servicio), lista) => ((marca, clase, servicio), lista.length)})


val dataGLPSorted = ListMap(dataGLP.toSeq.sortWith(_._2 > _._2):_*)

dataGLPSorted.foreach(row => printf("%s, %s, %s, %d\n",
  row._1._1,
  row._1._2,
  row._1._3,
  row._2))
new File("C:\\Users\\USUARIO\\dataglp.csv")
  .writeCsv[(String, String, String, Int)](
    dataGLPSorted.map(row => (row._1._1, row._1._2, row._1._3, row._2)),
    rfc.withHeader("marca", "clase", "servicio", "cantidad")
  )
//Consulta tres
val Combustible = values
  .map(row=> (row.marca, row.clase,row.combustible))
  .groupBy(identity)
  .map({ case((marca,clase,combustible), lista) => ((marca, clase,combustible), lista.length)})

val CombustibleSorted = ListMap(Combustible.toSeq.sortWith(_._2 > _._2):_*)

CombustibleSorted.foreach(row => printf("%s,%s, %s, %d\n",
  row._1._1,
  row._1._2,
  row._1._3,
  row._2))
new File("C:\\Users\\USUARIO\\dataCombustible.csv")
  .writeCsv[(String,String,String, Int)](
    CombustibleSorted.map(row => (row._1._1, row._1._2, row._1._3, row._2)),
    rfc.withHeader("marca","clase","combustible", "cantidad")
  )
//Consulta cuatro

val servicio = values
  .map(row=> (row.marca, row.clase, row.servicio))
  .groupBy(identity)
  .map({ case((marca, clase, servicio), lista) => ((marca, clase, servicio), lista.length)})


val servicioSorted = ListMap(servicio.toSeq.sortWith(_._2 > _._2):_*)

servicioSorted.foreach(row => printf("%s, %s, %s, %d\n",
  row._1._1,
  row._1._2,
  row._1._3,
  row._2))
new File("C:\\Users\\USUARIO\\dataservicio.csv")
  .writeCsv[(String, String, String, Int)](
    servicioSorted.map(row => (row._1._1, row._1._2, row._1._3, row._2)),
    rfc.withHeader("marca", "clase", "servicio", "cantidad")
  )

// Consulta Cinco
val clase = values
  .map(row=> (row.marca, row.clase, row.servicio))
  .groupBy(identity)
  .map({ case((marca, clase, servicio), lista) => ((marca, clase, servicio), lista.length)})


val claseSorted = ListMap(clase.toSeq.sortWith(_._2 > _._2):_*)

claseSorted.foreach(row => printf("%s, %s, %s, %d\n",
  row._1._1,
  row._1._2,
  row._1._3,
  row._2))
new File("C:\\Users\\USUARIO\\dataclase.csv")
  .writeCsv[(String, String, String, Int)](
    claseSorted.map(row => (row._1._1, row._1._2, row._1._3, row._2)),
    rfc.withHeader("marca", "clase", "servicio", "cantidad")
  )

//Consulta Seis
val motocicletas = values
  .filter(row => row.clase.startsWith("Motocicleta"))
  .map(row=> (row.marca, row.clase,row.provincia))
  .groupBy(identity)
  .map({ case((marca, clase,provincia), lista) => ((marca, clase, provincia), lista.length)})


val motocicletasSorted = ListMap(motocicletas.toSeq.sortWith(_._2 > _._2):_*)

motocicletasSorted.foreach(row => printf("%s, %s,%s, %d\n",
  row._1._1,
  row._1._2,
  row._1._3,
  row._2))
new File("C:\\Users\\Usuario\\datamotocicletas.csv")
  .writeCsv[(String, String,String, Int)](
    motocicletasSorted.map(row => (row._1._1, row._1._2,row._1._3, row._2)),
    rfc.withHeader("marca", "clase","provincia",  "cantidad")
  )
