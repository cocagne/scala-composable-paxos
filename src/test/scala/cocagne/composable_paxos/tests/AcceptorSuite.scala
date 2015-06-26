package cocagne.composable_paxos.tests

import org.scalatest._

import cocagne.composable_paxos._

class AcceptorSuite extends FunSuite with Matchers {

  test("Initial receive Prepare yields Promise") {
    val p = new Acceptor[String]("a")

    p.receive(Prepare("b", ProposalID(1, "b"))) should be(Some(Promise("a", ProposalID(1, "b"), "b", None, None)))
  }

  test("Duplicate Prepare yields identical Promise") {
    val p = new Acceptor[String]("a")
    p.receive(Prepare("b", ProposalID(1, "b"))) should be(Some(Promise("a", ProposalID(1, "b"), "b", None, None)))
    p.receive(Prepare("b", ProposalID(1, "b"))) should be(Some(Promise("a", ProposalID(1, "b"), "b", None, None)))
  }
  
  test("Prepare for less than promised yields nack") {
    val p = new Acceptor[String]("a")
    p.receive(Prepare("b", ProposalID(2, "b"))) should be(Some(Promise("a", ProposalID(2, "b"), "b", None, None)))
    p.receive(Prepare("b", ProposalID(1, "b"))) should be(Some(Nack("a", ProposalID(1,"b"), "b", Some(ProposalID(2,"b")))))
  }
  
  test("Prepare after accept includes promised and accepted values") {
    val p = new Acceptor[String]("a")
    p.receive(Prepare("b", ProposalID(1, "b"))) should be(Some(Promise("a", ProposalID(1, "b"), "b", None, None)))
    p.receive(Accept("b", ProposalID(1, "b"), "foo")) should be(Some(Accepted("a", ProposalID(1, "b"), "foo")))
    p.receive(Prepare("b", ProposalID(2, "b"))) should be(Some(Promise("a", ProposalID(2, "b"), "b", Some(ProposalID(1, "b")), Some("foo"))))
  }
  
  test("Accept without initial promise yields Accepted") {
    val p = new Acceptor[String]("a")
    p.receive(Accept("b", ProposalID(1, "b"), "foo")) should be(Some(Accepted("a", ProposalID(1, "b"), "foo")))
  }
  
  test("Accept for greater than promised yields accepted") {
    val p = new Acceptor[String]("a")
    p.receive(Prepare("b", ProposalID(1, "b"))) should be(Some(Promise("a", ProposalID(1, "b"), "b", None, None)))
    p.receive(Accept("b", ProposalID(2, "b"), "foo")) should be(Some(Accepted("a", ProposalID(2, "b"), "foo")))
  }
  
  test("Accept for less than promised yields nack") {
    val p = new Acceptor[String]("a")
    p.receive(Prepare("b", ProposalID(2, "b"))) should be(Some(Promise("a", ProposalID(2, "b"), "b", None, None)))
    p.receive(Accept("b", ProposalID(1, "b"), "foo")) should be(Some(Nack("a", ProposalID(1,"b"), "b", Some(ProposalID(2,"b")))))
  }
}