package cocagne.composable_paxos

class Proposer[T] (val networkUid: NetworkUID, val quorumSize: Int) {
  
  private var _proposedValue:    Option[T]           = None
  private var _proposalId                            = ProposalID(0, networkUid)
  private var highestProposalId                      = ProposalID(0, networkUid)
  private var highestAcceptedId: Option[ProposalID]  = None
  private var promisesReceived:  Set[NetworkUID]     = Set()
  private var nacksReceived:     Set[NetworkUID]     = Set()
  private var _currentPrepare:   Option[Prepare]     = None
  private var _currentAccept:    Option[Accept[T]]   = None
    
  var leader         = false
  def proposedValue  = _proposedValue
  def proposalId     = _proposalId
  def currentPrepare = _currentPrepare
  def currentAccept  = _currentAccept
  
  
  def proposeValue(value: T): Option[Accept[T]] = {
    proposedValue match {
      case None => _proposedValue = Some(value)
        if (leader) {
          _currentAccept = Some(Accept(networkUid, _proposalId, value))
          _currentAccept
        } else
          None
      case Some(_) => None
    } 
  }

  
  def prepare() : Prepare = {
    leader            = false
    promisesReceived  = Set()
    nacksReceived     = Set()
    _proposalId       = ProposalID(highestProposalId.proposalNumber + 1, networkUid)
    highestProposalId = _proposalId
    _currentPrepare   = Some(Prepare(networkUid, proposalId))
    
    _currentPrepare.get
  }

  
  def observeProposal(proposalId: ProposalID) : Unit = {
    if (proposalId > highestProposalId)
      highestProposalId = proposalId
  }
            
            
  def receive(msg: Message) : Option[Message] = msg match {
    case n: Nack => receiveNack(n)
    case p: Promise[T] => receivePromise(p)
    case _ => None
  }

  
  private def receiveNack(msg: Nack) : Option[Prepare] = {
    msg.promisedProposalId foreach observeProposal
    
    if (msg.proposalId == proposalId) {
      nacksReceived = nacksReceived + msg.networkUid
      if (nacksReceived.size == quorumSize)
        Some(prepare)
      else
        None
    }
    else
      None
  }

  
  private def receivePromise(msg: Promise[T]): Option[Accept[T]] = {
    observeProposal(msg.proposalId)
    
    if (!leader && msg.proposalId == proposalId && !promisesReceived.contains(msg.networkUid)) {
      promisesReceived = promisesReceived + msg.networkUid
      
      def updateAccepted(pid: ProposalID, value: Option[T]) = {
        highestAcceptedId = Some(pid)
        
        value match {
          case Some(v) => _proposedValue = Some(v)
          case None =>
        }
      }
      
      (msg.lastAcceptedProposalId, highestAcceptedId) match {
        case (Some(pid), None) => updateAccepted(pid, msg.lastAcceptedValue)
        case (Some(pid), Some(acceptedId)) if pid > acceptedId => updateAccepted(pid, msg.lastAcceptedValue)
        case _ =>
      }
      
      if (promisesReceived.size == quorumSize) {
        leader = true
        proposedValue match {
          case Some(value) => _currentAccept = Some(Accept(networkUid, proposalId, value))
            _currentAccept
          case None => None
        }
      }
      else
        None
    }
    else
      None
  } 

}