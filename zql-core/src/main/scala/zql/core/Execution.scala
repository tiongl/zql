package zql.core

import java.util.concurrent.atomic.AtomicLong

/** A step that is executable to an output **/
abstract class ExecutableStep[OUT] {
  def execute(): OUT

  def next[U](desc: String)(func: OUT => U) = {
    new ChainedStep(desc, this, func)
  }
}

/** An executable with a desc **/
class IdentityStep[T](desc: String, t: => T) extends ExecutableStep[T] {
  override def execute(): T = t
}

/** An executable step with prev step **/
class ChainedStep[IN, OUT](val desc: String, prev: ExecutableStep[IN], func: (IN) => OUT) extends ExecutableStep[OUT] {
  override def execute(): OUT = {
    "Execuing " + desc
    val in = prev.execute()
    func(in)
  }
}

/**
 * Represent an execution plan
 *
 * @param desc
 */
class ExecutionPlan[That](desc: String, lastStep: ExecutableStep[That]) extends Executable[That] {

  /** a unique id **/
  val id = ExecutionPlan.ID_COUNTER.addAndGet(1)

  def execute(): That = lastStep.execute()
}

object ExecutionPlan {
  /** id counter **/
  val ID_COUNTER = new AtomicLong()

  def plan[T](name: String)(planFunc: => ExecutableStep[T]): ExecutionPlan[T] = {
    new ExecutionPlan(name, planFunc)
  }

  def first[That](name: String)(func: => That): IdentityStep[That] = {
    val step = new IdentityStep(name, func)
    step
  }

  def operate[T](data: T) = {
    println(data)
  }

  def main(args: Array[String]) {
    val p = plan("test") {
      first("One") {
        1
      }.next("Two") {
        _ + 1
      }
    }
    println(p)
    operate(1)
    val i = 10
  }
}