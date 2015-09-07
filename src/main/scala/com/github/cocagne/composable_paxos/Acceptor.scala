package com.github.cocagne.composable_paxos

class Acceptor[T](val networkUid: NetworkUID) {
  private var _promisedId:    Option[ProposalID] = None
  private var _acceptedId:    Option[ProposalID] = None
  private var _acceptedValue: Option[T]          = None
  
  def promisedId    = _promisedId
  def acceptedId    = _acceptedId
  def acceptedValue = _acceptedValue

  
  def receive(msg: Message): Option[Message] = msg match {
    case p: Prepare   => Some(receivePrepare(p))
    case a: Accept[T] => Some(receiveAccept(a))
    case _ => None
  }

  
  private def receivePrepare(msg: Prepare) = {
    def promise() = {
      _promisedId = Some(msg.proposalId)
      Promise(networkUid, msg.proposalId, msg.networkUid, _acceptedId, _acceptedValue)
    }

    _promisedId match {
      case Some(promised) if msg.proposalId >= promised => promise
      case None => promise
      case _ => Nack(networkUid, msg.proposalId, msg.networkUid, _promisedId)
    }
  }

  
  private def receiveAccept(msg: Accept[T]) = {
    def accept() = {
      _promisedId    = Some(msg.proposalId)
      _acceptedId    = Some(msg.proposalId)
      _acceptedValue = Some(msg.proposalValue)
      
      Accepted(networkUid, msg.proposalId, msg.proposalValue)
    }
    
    _promisedId match {  
      case Some(promised) if msg.proposalId >= promised => accept
      case None => accept 
      case _ => Nack(networkUid, msg.proposalId, msg.networkUid, _promisedId)
    }
  }
}