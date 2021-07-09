package com.jd.atlas.process

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import org.apache.spark.internal.Logging

import scala.reflect.ClassTag
import scala.util.control.NonFatal

abstract class AbstractProcessor[T: ClassTag] extends Logging{
    val eventQueue = new LinkedBlockingQueue[T](1000)
    val timeout=10000L;
  private val eventProcessThread = new Thread {
    override def run(): Unit = {
        eventProcess()
    }
  }
  def pushEvent(event: T): Unit = {
    event match {
      case e: T =>
        if (!eventQueue.offer(e, timeout, TimeUnit.MILLISECONDS)) {
          logError(s"添加 $e 到队列失败，由于超过了 $timeout, 因此将要丢弃该事件")
        }
      case _ => // Ignore other events
    }
  }
  def startThread(): Unit = {
    eventProcessThread.setName(this.getClass.getSimpleName + "-thread")
    eventProcessThread.setDaemon(true)
    val ctxClassLoader = Thread.currentThread().getContextClassLoader
    if (ctxClassLoader != null && getClass.getClassLoader != ctxClassLoader) {
      eventProcessThread.setContextClassLoader(ctxClassLoader)
    }
    eventProcessThread.start()
  }

  protected def process(e: T): Unit

  private def eventProcess(): Unit = {
    var stopped = false
    while (!stopped) {
      try {
        Option(eventQueue.poll(3000, TimeUnit.MILLISECONDS)).foreach { e =>
            process(e)
        }
      } catch {
        case _: InterruptedException =>
          logDebug("Thread is interrupted")
          stopped = true
        case NonFatal(f) =>
          logWarning(s"在解析事件期间捕获到异常", f)
      }
    }
  }
 sys.addShutdownHook(()=>{
   eventProcessThread
 })
}
