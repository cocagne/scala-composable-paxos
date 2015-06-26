package cocagne.composable_paxos.tests

import org.scalatest._

import cocagne.composable_paxos._

class ProposerSuite extends FunSuite with Matchers {
  
	test("Propose value yields no Accept message while not the leader") {
	  val p = new Proposer[String]("a",2)
	  
	  p.proposedValue should be (None)
	  
	  p.proposeValue("foo") should be (None)
	}
	
	test("Propose value as leader yields Accept message") {
	  val p = new Proposer[String]("a",2)
	  
	  p.leader = true
	  
	  p.proposedValue should be (None)
	  
	  p.proposeValue("foo") should be (Some(Accept("a", ProposalID(0,"a"), "foo")))
	}
	
	test("Propose value ignores additional proposed values") {
	  val p = new Proposer[String]("a",2)
	  
	  p.proposeValue("foo")
	  p.proposeValue("bar")
	  
	  p.proposedValue should be (Some("foo"))
	}
	
	test("Prepare increments proposal id") {
	  val p = new Proposer[String]("a",2)
	  
	  p.currentPrepare should be (None)
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  p.currentPrepare should be (Some(Prepare("a", ProposalID(1,"a"))))
	  
	  p.prepare() should equal (Prepare("a", ProposalID(2,"a")))
	  p.currentPrepare should be (Some(Prepare("a", ProposalID(2,"a"))))
	}
	
	test("Nacks induce new call to prepare") {
	  val p = new Proposer[String]("a",2)
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  
	  p.receive(Nack("b", ProposalID(1,"a"), "a", Some(ProposalID(10,"b")))) should be (None)
	  p.receive(Nack("c", ProposalID(1,"a"), "a", Some(ProposalID(10,"b")))) should be (Some(Prepare("a", ProposalID(11,"a"))))
	}

	test("Nacks cause leadership loss") {
	  val p = new Proposer[String]("a",2)
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  
	  p.leader = true
	  
	  p.receive(Nack("b", ProposalID(1,"a"), "a", Some(ProposalID(10,"b")))) should be (None)
	  p.receive(Nack("c", ProposalID(1,"a"), "a", Some(ProposalID(10,"b")))) should be (Some(Prepare("a", ProposalID(11,"a"))))
	  
	  assert(!p.leader)
	}
	
	test("Leadership Acquisition without proposal") {
	  val p = new Proposer[String]("a",2)
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  
	  p.receive(Promise("b", ProposalID(1,"a"), "a", None, None)) should be (None)
	  p.receive(Promise("c", ProposalID(1,"a"), "a", None, None)) should be (None)
	  
	  assert(p.leader)
	}
	
	test("Leadership Acquisition with proposal") {
	  val p = new Proposer[String]("a",2)
	  
	  p.proposeValue("foo")
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  
	  p.receive(Promise("b", ProposalID(1,"a"), "a", None, None)) should be (None)
	  p.receive(Promise("c", ProposalID(1,"a"), "a", None, None)) should be (Some(Accept("a", ProposalID(1,"a"), "foo")))
	  
	  assert(p.leader)
	}
	
	test("Previous accepted value overrides local value") {
	  val p = new Proposer[String]("a",2)
	  
	  p.proposeValue("foo")
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  p.prepare() should equal (Prepare("a", ProposalID(2,"a")))
	  
	  p.receive(Promise("b", ProposalID(2,"a"), "a", Some(ProposalID(1,"b")), Some("bar"))) should be (None)
	  p.receive(Promise("c", ProposalID(2,"a"), "a", None, None)) should be (Some(Accept("a", ProposalID(2,"a"), "bar")))
	  
	  p.proposedValue should be (Some("bar"))
	  
	  assert(p.leader)
	}
	
	test("Previous accepted id but with null value does not override local value") {
	  val p = new Proposer[String]("a",2)
	  
	  p.proposeValue("foo")
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  p.prepare() should equal (Prepare("a", ProposalID(2,"a")))
	  
	  p.receive(Promise("b", ProposalID(2,"a"), "a", Some(ProposalID(1,"b")), None)) should be (None)
	  p.receive(Promise("c", ProposalID(2,"a"), "a", None, None)) should be (Some(Accept("a", ProposalID(2,"a"), "foo")))
	  
	  p.proposedValue should be (Some("foo"))
	  
	  assert(p.leader)
	}
	
	test("Newer previous accepted value does not override local value") {
	  val p = new Proposer[String]("a",2)
	  
	  p.proposeValue("foo")
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  p.prepare() should equal (Prepare("a", ProposalID(2,"a")))
	  p.prepare() should equal (Prepare("a", ProposalID(3,"a")))
	  
	  p.receive(Promise("b", ProposalID(3,"a"), "a", Some(ProposalID(2,"b")), Some("bar"))) should be (None)
	  p.receive(Promise("c", ProposalID(3,"a"), "a", Some(ProposalID(1,"c")), Some("baz"))) should be (Some(Accept("a", ProposalID(3,"a"), "bar")))
	  
	  p.proposedValue should be (Some("bar"))
	  
	  assert(p.leader)
	}
	
	test("Mixed proposal_id Promises do not trigger leadership acquisition") {
	  val p = new Proposer[String]("a",2)
	  
	  p.proposeValue("foo")
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  
	  p.receive(Promise("b", ProposalID(1,"a"), "a", None, None)) should be (None)
	  
	  p.prepare() should equal (Prepare("a", ProposalID(2,"a")))
	  
	  p.receive(Promise("b", ProposalID(2,"a"), "a", None, None)) should be (None)
	  p.receive(Promise("c", ProposalID(2,"a"), "a", None, None)) should be (Some(Accept("a", ProposalID(2,"a"), "foo")))
	  
	  assert(p.leader)
	}
	
	test("Ignore extraneous promises") {
	  val p = new Proposer[String]("a",2)
	  
	  p.proposeValue("foo")
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  
	  p.receive(Promise("b", ProposalID(1,"a"), "a", None, None)) should be (None)
	  
	  p.receive(Promise("c", ProposalID(1,"b"), "a", None, None)) should be (None)
	  p.receive(Promise("c", ProposalID(2,"a"), "a", None, None)) should be (None)
	  p.receive(Promise("c", ProposalID(0,"a"), "a", None, None)) should be (None)
	  p.receive(Promise("b", ProposalID(2,"a"), "a", None, None)) should be (None)
	  
	  
	  p.receive(Promise("c", ProposalID(1,"a"), "a", None, None)) should be (Some(Accept("a", ProposalID(1,"a"), "foo")))
	  
	  assert(p.leader)
	}
	
	test("Ignore duplicate promises") {
	  val p = new Proposer[String]("a",2)
	  
	  p.proposeValue("foo")
	  
	  p.prepare() should equal (Prepare("a", ProposalID(1,"a")))
	  
	  p.receive(Promise("b", ProposalID(1,"a"), "a", None, None)) should be (None)
	  p.receive(Promise("b", ProposalID(1,"a"), "a", None, None)) should be (None)
	  
	  p.receive(Promise("c", ProposalID(1,"a"), "a", None, None)) should be (Some(Accept("a", ProposalID(1,"a"), "foo")))
	  
	  assert(p.leader)
	}
}