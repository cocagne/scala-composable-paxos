package cocagne.composable_paxos


case class ProposalID(proposalNumber: Int, networkUid: NetworkUID) extends Ordered[ProposalID] {
  def compare(that: ProposalID): Int = if (this.proposalNumber < that.proposalNumber) 
    -1
  else if (this.proposalNumber == that.proposalNumber)
    this.networkUid.compare(that.networkUid)
  else
    1
}

sealed abstract class Message

case class Prepare(networkUid: NetworkUID, proposalId: ProposalID) extends Message

case class Nack(networkUid: NetworkUID, proposalId: ProposalID,
    proposerUid: NetworkUID, promisedProposalId: Option[ProposalID]) extends Message
    
case class Promise[T](networkUid: NetworkUID, proposalId: ProposalID,
    proposerUid: NetworkUID, lastAcceptedProposalId: Option[ProposalID], 
    lastAcceptedValue: Option[T]) extends Message
        
case class Accept[T](networkUid: NetworkUID, proposalId: ProposalID, 
    proposalValue: T) extends Message

case class Accepted[T](networkUid: NetworkUID, proposalId: ProposalID, 
    proposalValue: T) extends Message        

case class Resolution[T](networkUid: NetworkUID, proposalValue: T) extends Message

