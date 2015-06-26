package cocagne.composable_paxos.tests

import org.scalatest._

import cocagne.composable_paxos._

class LearnerSuite extends FunSuite with Matchers {

  test("Basic resolution") {
    val l = new Learner[String]("a",2)
    assert(l.quorumSize == 2)
    l.finalValue should be (None)
    
    l.receive( Accepted("a",ProposalID(1,"a"), "foo" )) should be (None)
    l.finalValue should be (None)
   
    l.receive( Accepted("b",ProposalID(1,"a"), "foo" )) should be (Some(Resolution("a", "foo")))
    l.finalValue should be (Some("foo"))
    l.finalAcceptors should be (Set("a","b"))
  }
  

  test("Ignore messages after resolution"){
    val l = new Learner[String]("a",2)
    
    l.receive( Accepted("a",ProposalID(1,"a"), "foo" )) should be (None)
    l.finalValue should be (None)
   
    l.receive( Accepted("b",ProposalID(1,"a"), "foo" )) should be (Some(Resolution("a", "foo")))
    l.finalValue should be (Some("foo"))
    l.finalAcceptors should be (Set("a","b"))
    
    l.receive( Accepted("b",ProposalID(5,"a"), "BAR" )) should be (Some(Resolution("a", "foo")))
    l.receive( Accepted("c",ProposalID(5,"a"), "BAR" )) should be (Some(Resolution("a", "foo")))
    
    l.finalAcceptors should be (Set("a","b"))
  }    
  
  test("Accumulate acceptors after resolution"){
    val l = new Learner[String]("a",2)
    
    l.receive( Accepted("a",ProposalID(1,"a"), "foo" )) should be (None)
    l.finalValue should be (None)
   
    l.receive( Accepted("b",ProposalID(1,"a"), "foo" )) should be (Some(Resolution("a", "foo")))
    l.finalValue should be (Some("foo"))
    l.finalAcceptors should be (Set("a","b"))
    
    l.receive( Accepted("c",ProposalID(1,"a"), "foo" )) should be (Some(Resolution("a", "foo")))
    
    l.finalAcceptors should be (Set("a","b","c"))
  }    

  
  test("Ignore duplicate messages") {
    val l = new Learner[String]("a",2)
    
    l.receive( Accepted("a",ProposalID(1,"a"), "foo" )) should be (None)
    l.receive( Accepted("a",ProposalID(1,"a"), "foo" )) should be (None)
    l.finalValue should be (None)
   
    l.receive( Accepted("b",ProposalID(1,"a"), "foo" )) should be (Some(Resolution("a", "foo")))
    l.finalValue should be (Some("foo"))
    l.finalAcceptors should be (Set("a","b"))
  }

  
  test("Ignore old messages") {
    val l = new Learner[String]("a",2)
    
    l.receive( Accepted("a",ProposalID(5,"a"), "foo" )) should be (None)
    l.receive( Accepted("a",ProposalID(1,"a"), "foo" )) should be (None)
    l.finalValue should be (None)
   
    l.receive( Accepted("b",ProposalID(5,"a"), "foo" )) should be (Some(Resolution("a", "foo")))
    l.finalValue should be (Some("foo"))
    l.finalAcceptors should be (Set("a","b"))
  }
  
  test("New proposal removes acceptor from set but still resolves") {
    val l = new Learner[String]("a",3)
    
    l.receive( Accepted("a",ProposalID(5,"a"), "foo" )) should be (None)
    l.receive( Accepted("b",ProposalID(5,"a"), "foo" )) should be (None)
    l.receive( Accepted("a",ProposalID(6,"a"), "foo" )) should be (None)
    l.finalValue should be (None)
   
    l.receive( Accepted("c",ProposalID(5,"a"), "foo" )) should be (Some(Resolution("a", "foo")))
    l.finalValue should be (Some("foo"))
    l.finalAcceptors should be (Set("b","c"))
  }

  
  test("Resolve with mixed proposal versions") {
    val l = new Learner[String]("a",2)
    
    l.receive( Accepted("a",ProposalID(5,"a"), "foo" )) should be (None)
    l.receive( Accepted("b",ProposalID(1,"a"), "foo" )) should be (None)
    l.finalValue should be (None)
   
    l.receive( Accepted("c",ProposalID(1,"a"), "foo" )) should be (Some(Resolution("a", "foo")))
    l.finalValue should be (Some("foo"))
    l.finalAcceptors should be (Set("b","c"))
  }

  
  test("Overwrite old messages") {
    val l = new Learner[String]("a",2)
    
    l.receive( Accepted("a",ProposalID(1,"a"), "foo" )) should be (None)
    l.receive( Accepted("b",ProposalID(5,"a"), "foo" )) should be (None)
    l.finalValue should be (None)
   
    l.receive( Accepted("a",ProposalID(5,"a"), "foo" )) should be (Some(Resolution("a", "foo")))
    l.finalValue should be (Some("foo"))
    l.finalAcceptors should be (Set("a","b"))
  }
}