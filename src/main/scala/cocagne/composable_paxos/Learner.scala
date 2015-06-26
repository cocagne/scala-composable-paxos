package cocagne.composable_paxos

class Learner[T](val networkUid: NetworkUID, val quorumSize: Int) {
  
  private case class ProposalStatus(var acceptCount: Int, var retainCount: Int, 
      var acceptors: Set[NetworkUID], val value: T)
  
  private var proposals: Map[ProposalID, ProposalStatus] = Map() // proposal_id -> [accept_count, retain_count, value]
  private var acceptors: Map[NetworkUID, ProposalID]     = Map() // from_uid -> last_accepted_proposal_id
  private var _finalValue: Option[T]                     = None
  private var _finalAcceptors: Set[NetworkUID]           = Set() // Contains UIDs of all acceptors known to have the final value
  private var _finalProposalId: Option[ProposalID]       = None
  
  def finalValue      = _finalValue
  def finalAcceptors  = _finalAcceptors
  def finalProposalId = _finalProposalId 

  def receive(msg: Accepted[T]): Option[Resolution[T]] = {
    
    _finalValue match {
      case Some(value) => 
        if (msg.proposalId >= _finalProposalId.get && msg.proposalValue == _finalValue.get)
          _finalAcceptors += msg.networkUid
        return Some(Resolution(networkUid, _finalValue.get))
      case None =>
    }
    
    val last = acceptors.get(msg.networkUid)
    
    last match {
      case Some(lastPid) if msg.proposalId <= lastPid => return None // Old message
      case _ => 
    }
    
    acceptors += (msg.networkUid -> msg.proposalId)
    
    last match {
      case Some(lastPid) => 
        val pstatus = proposals.get(lastPid).get
        pstatus.retainCount -= 1
        pstatus.acceptors -= msg.networkUid
        if (pstatus.retainCount == 0)
          proposals -= lastPid
      case _ =>
    }
    
    if ( !proposals.contains(msg.proposalId) )
      proposals += (msg.proposalId -> ProposalStatus(0,0,Set[NetworkUID](),msg.proposalValue))
    
    val pstatus = proposals.get(msg.proposalId).get
    pstatus.acceptCount += 1
    pstatus.retainCount += 1
    pstatus.acceptors   += msg.networkUid
    
    if (pstatus.acceptCount == quorumSize) {
      _finalProposalId = Some(msg.proposalId)
      _finalValue      = Some(msg.proposalValue)
      _finalAcceptors  = pstatus.acceptors
      
      proposals = proposals.empty
      acceptors = acceptors.empty
      
      Some(Resolution(networkUid, msg.proposalValue))
    }
    else
      None
  }
}