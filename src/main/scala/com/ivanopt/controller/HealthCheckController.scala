package com.ivanopt.controller

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation._

@RestController
class HealthCheckController() {

  @RequestMapping(value = Array("/health"), method = Array(RequestMethod.GET))
  @ResponseStatus(HttpStatus.OK)
  def accounts() = "health"

}
