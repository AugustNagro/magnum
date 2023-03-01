package com.augustnagro.magnum

trait SeekDir:
  def sql: String

object SeekDir:
  object Gt extends SeekDir:
    def sql: String = ">"

  object Lt extends SeekDir:
    def sql: String = "<"
