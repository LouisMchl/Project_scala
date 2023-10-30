import scala.collection.mutable.HashMap

class Table(val tableName: TableName, val schema: Schema, val store: Store) extends TableAPI:

override def insert(values: Map[ColumnName, Any]): Either[DatabaseError, Unit] = {
  // Validez les valeurs et le schéma.
  if (!validateSchema(values)) {
    return Left(DatabaseError.ValidationFailed("Invalid schema for table"))
  }

  // Générez les clés et les valeurs pour le KV-Store "data_<nom-table>".
  val keyValuePairs = values.flatMap { case (columnName, value) =>
    schema.columns.find(_.name == columnName).map { colDeclaration =>
      val key = s"${values("id")}#$columnName"
      val valueString = value.toString
      (key, valueString)
    }
  }

  // Insérez les clés et les valeurs dans le KV-Store "data_<nom-table>".
  keyValuePairs.foreach { case (key, valueString) =>
    store.put(s"data_$tableName", key, valueString)
  }

  Right(())
}

override def execute(executionPlan: ExecutionPlan): Either[DatabaseError, Result] = {
  // Implémentez la logique pour exécuter le plan d'exécution sur la table.
  // Vous devrez interagir avec le KV-Store "data_<nom-table>" pour lire ou écrire des données.
  // Gérez les erreurs de base de données si nécessaire.
  // Assurez-vous que le plan d'exécution est compatible avec le schéma de la table.
  // Utilisez le schéma pour valider les opérations.

  // Exemple de code :
  // if (!validateExecutionPlan(executionPlan)) {
  //   return Left(DatabaseError.ValidationFailed("Invalid execution plan"))
  // }

  // Écrire la logique d'exécution du plan ici.

  // Fournir le résultat au format Result.
  // Result(columns, rows)
}

// Validez le schéma en fonction du type de colonnes.
private def validateSchema(values: Map[ColumnName, Any]): Boolean = {
  schema.columns.forall { colDeclaration =>
    values.get(colDeclaration.name) match {
      case Some(value) => validateColumnType(value, colDeclaration.colType)
      case None => false
    }
  }
}

// Validez le type de la colonne en fonction de sa déclaration.
private def validateColumnType(value: Any, colType: ColumnType): Boolean = {
  colType match {
    case ColumnType.IntType => value.isInstanceOf[Int]
    case ColumnType.StringType => value.isInstanceOf[String]
    case ColumnType.BooleanType => value.isInstanceOf[Boolean]
    case ColumnType.TimestampType => value.isInstanceOf[String] // Timestamp est stocké sous forme de chaîne
  }
}
}
