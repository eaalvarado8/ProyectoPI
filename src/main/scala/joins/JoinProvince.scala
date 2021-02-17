package joins

import slick.jdbc.MySQLProfile.api._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object JoinProvince extends App{
  //Representa a la fila
  case class Province(name: String, id: Long=0L)

  //Representa a la tabla
  class ProvinceTable(tag: Tag) extends Table[Province](tag, "PROVINCE") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def name = column[String]("NAME")

    def * = (name, id).mapTo[Province]
  }
  lazy val qryProvince = TableQuery[ProvinceTable]
  lazy val insertProvince = qryProvince returning qryProvince.map(_.id)

  case class VehicleRegistation(modelo: String, tonelaje: Double, asientos: Int, provinceId: Long, id: Long=0L)

  class VehicleRegistrationTable(tag: Tag) extends Table[VehicleRegistation](tag, "VEHICLE_REGISTRATION") {
    def id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    def provinceId = column[Long]("PROVINCE_ID")
    def modelo = column[String]("MODELO")
    def tonelaje = column[Double]("TONELAJE")
    def asientos = column[Int]("ASIENTOS")

    def province = foreignKey("province_fk", provinceId, qryProvince)(_.id)

    def * = (modelo, tonelaje, asientos, provinceId,id).mapTo[VehicleRegistation]
  }
  lazy val registrationQry = TableQuery[VehicleRegistrationTable]
  lazy val insertVehicleRegistration = registrationQry returning registrationQry.map(_.id)

  val db = Database.forConfig("test01")
  def exec[T](program:DBIO[T]):T = Await.result(db.run(program), 100 seconds)

  exec((qryProvince.schema ++ registrationQry.schema).create)

}
