package com.augustnagro.magnum

trait SeekDir:
  def sql: String

object SeekDir:
  val Gt: SeekDir = new:
    def sql: String = ">"

  val Lt: SeekDir = new:
    def sql: String = "<"
