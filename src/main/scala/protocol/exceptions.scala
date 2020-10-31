package scalackh.protocol

class ClickhouseClientException(message: String) extends RuntimeException(message)
class ClickhouseServerException(cause: ServerException) extends RuntimeException {
  override def toString(): String = s"ClickhouseServerException(${cause.toString()})"
  override def getMessage(): String = cause.message
}