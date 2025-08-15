import { useState, useEffect } from 'react';
import { Button } from '../../ui/button';
import { ArrowLeft, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { KafkaCluster } from '../types';
import { invoke } from '@tauri-apps/api/core';

interface ClusterConfigModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  cluster?: KafkaCluster | null;
  mode: 'create' | 'edit';
  onBack?: () => void;
  onSave?: (cluster: Partial<KafkaCluster>) => void;
  onConnected?: (topics: string[]) => void;
}

export function ClusterConfigModal({ 
  open, 
  onOpenChange, 
  cluster, 
  mode,
  onBack,
  onSave,
  onConnected,
}: ClusterConfigModalProps) {
  const [name, setName] = useState<string>('');
  const [brokers, setBrokers] = useState<string>('localhost:9092');
  const [securityProtocol, setSecurityProtocol] = useState<string>('PLAINTEXT');
  const [username, setUsername] = useState<string>('');
  const [password, setPassword] = useState<string>('');
  const [keystorePath, setKeystorePath] = useState<string>('');
  const [keystorePassword, setKeystorePassword] = useState<string>('');
  const [truststorePath, setTruststorePath] = useState<string>('');
  const [truststorePassword, setTruststorePassword] = useState<string>('');
  const [saslMechanism, setSaslMechanism] = useState<string>('PLAIN');

  const [isConnecting, setIsConnecting] = useState<boolean>(false);
  const [isTesting, setIsTesting] = useState<boolean>(false);

  // Load cluster data when editing
  useEffect(() => {
    if (cluster && mode === 'edit') {
      setName(cluster.name);
      setBrokers(cluster.brokers);
      setSecurityProtocol(cluster.securityProtocol);
      setUsername(cluster.username || '');
      setPassword(cluster.password || '');
      setKeystorePath(cluster.keystorePath || '');
      setKeystorePassword(cluster.keystorePassword || '');
      setTruststorePath(cluster.truststorePath || '');
      setTruststorePassword(cluster.truststorePassword || '');
      setSaslMechanism(cluster.saslMechanism || 'PLAIN');
    } else {
      // Reset form for create mode
      setName('');
      setBrokers('localhost:9092');
      setSecurityProtocol('PLAINTEXT');
      setUsername('');
      setPassword('');
      setKeystorePath('');
      setKeystorePassword('');
      setTruststorePath('');
      setTruststorePassword('');
      setSaslMechanism('PLAIN');
    }
  }, [cluster, mode, open]);

  const handleConnect = async () => {
    const clusterData = {
      name,
      brokers,
      security_protocol: securityProtocol,
      sasl_mechanism: isSASLRequired ? saslMechanism : undefined,
      username: isSASLRequired ? username : undefined,
      password: isSASLRequired ? password : undefined,
      keystore_path: isSSLRequired ? keystorePath : undefined,
      keystore_password: isSSLRequired ? keystorePassword : undefined,
      truststore_path: isSSLRequired ? truststorePath : undefined,
      truststore_password: isSSLRequired ? truststorePassword : undefined,
    };

    try {
      setIsConnecting(true);
      await invoke('cluster_connect', { payload: clusterData });
      // After successful connect, fetch topics from backend
      try {
        const topics = await invoke<string[]>('get_topics');
        if (onConnected) {
          onConnected(topics);
        }
      } catch (err) {
        console.error('Failed to fetch topics after connect', err);
      }
      toast.success('Connected to Kafka cluster');
      onOpenChange(false);
    } catch (e) {
      console.error(e);
      toast.error('Failed to connect to Kafka cluster');
    } finally {
      setIsConnecting(false);
    }
  };

  const handleTestConnection = async () => {
    const clusterData = {
      name,
      brokers,
      security_protocol: securityProtocol,
      sasl_mechanism: isSASLRequired ? saslMechanism : undefined,
      username: isSASLRequired ? username : undefined,
      password: isSASLRequired ? password : undefined,
      keystore_path: isSSLRequired ? keystorePath : undefined,
      keystore_password: isSSLRequired ? keystorePassword : undefined,
      truststore_path: isSSLRequired ? truststorePath : undefined,
      truststore_password: isSSLRequired ? truststorePassword : undefined,
    };

    try {
      setIsTesting(true);
      await invoke('cluster_test', { payload: clusterData });
      toast.success('Connection test successful');
    } catch (e) {
      console.error(e);
      toast.error('Connection test failed');
    } finally {
      setIsTesting(false);
    }
  };

  const handleSave = () => {
    const clusterData: Partial<KafkaCluster> = {
      id: cluster?.id,
      name,
      brokers,
      securityProtocol,
      saslMechanism: isSASLRequired ? saslMechanism : undefined,
      username: isSASLRequired ? username : undefined,
      password: isSASLRequired ? password : undefined,
      keystorePath: isSSLRequired ? keystorePath : undefined,
      keystorePassword: isSSLRequired ? keystorePassword : undefined,
      truststorePath: isSSLRequired ? truststorePath : undefined,
      truststorePassword: isSSLRequired ? truststorePassword : undefined,
      createdAt: cluster?.createdAt || new Date().toISOString(),
    };

    if (onSave) {
      onSave(clusterData);
    }
    
    toast.success(mode === 'create' ? 'Cluster created successfully' : 'Cluster updated successfully');
    onOpenChange(false);
  };

  const handleBack = () => {
    if (onBack) {
      onBack();
    }
  };

  const isSSLRequired = securityProtocol === 'SSL' || securityProtocol === 'SASL_SSL';
  const isSASLRequired = securityProtocol === 'SASL_PLAINTEXT' || securityProtocol === 'SASL_SSL';

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContentNoClose className="max-w-2xl max-h-[90vh] bg-[#0f172b] border-[#314158] text-slate-50 flex flex-col">
        <DialogHeader className="border-b border-[#314158] pb-4 flex-shrink-0">
          <div className="flex items-center gap-3">
            {onBack && (
              <Button
                onClick={handleBack}
                variant="ghost"
                size="sm"
                className="text-[#90a1b9] hover:text-[#ffb86a] hover:bg-[#314158] p-2"
              >
                <ArrowLeft className="size-4" />
              </Button>
            )}
            <div>
              <DialogTitle className="font-['Fira_Code:Retina',_sans-serif] text-[#90a1b9] text-lg">
                {mode === 'create' ? 'Create New Cluster' : 'Edit Cluster'}
              </DialogTitle>
              <DialogDescription className="text-sm text-[#90a1b9]">
                Configure your Kafka cluster connection settings
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>
        
        <div className="flex-1 overflow-auto">
          <div className="space-y-6 p-1 pr-3">
            {/* Cluster Name */}
            <div className="space-y-2">
              <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                Cluster Name
              </Label>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="My Kafka Cluster"
                className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
              />
            </div>

            {/* Brokers */}
            <div className="space-y-2">
              <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                Bootstrap Servers
              </Label>
              <Input
                value={brokers}
                onChange={(e) => setBrokers(e.target.value)}
                placeholder="localhost:9092"
                className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
              />
            </div>

            {/* Security Protocol */}
            <div className="space-y-2">
              <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                Security Protocol
              </Label>
              <Select value={securityProtocol} onValueChange={setSecurityProtocol}>
                <SelectTrigger className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-[#0f172b] border-[#314158]">
                  <SelectItem value="PLAINTEXT" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">PLAINTEXT</SelectItem>
                  <SelectItem value="SSL" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">SSL</SelectItem>
                  <SelectItem value="SASL_PLAINTEXT" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">SASL_PLAINTEXT</SelectItem>
                  <SelectItem value="SASL_SSL" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">SASL_SSL</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* SASL Configuration */}
            {isSASLRequired && (
              <div className="space-y-4 border border-[#314158] rounded-lg p-4">
                <h3 className="font-['Fira_Code:Retina',_sans-serif] text-[#ffb86a] text-sm">SASL Configuration</h3>
                
                <div className="space-y-2">
                  <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                    SASL Mechanism
                  </Label>
                  <Select value={saslMechanism} onValueChange={setSaslMechanism}>
                    <SelectTrigger className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif]">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-[#0f172b] border-[#314158]">
                      <SelectItem value="PLAIN" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">PLAIN</SelectItem>
                      <SelectItem value="SCRAM-SHA-256" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">SCRAM-SHA-256</SelectItem>
                      <SelectItem value="SCRAM-SHA-512" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">SCRAM-SHA-512</SelectItem>
                      <SelectItem value="GSSAPI" className="text-slate-50 font-['Fira_Code:Retina',_sans-serif] focus:bg-[#314158]">GSSAPI</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                      Username
                    </Label>
                    <Input
                      value={username}
                      onChange={(e) => setUsername(e.target.value)}
                      placeholder="Enter username"
                      className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                      Password
                    </Label>
                    <Input
                      type="password"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      placeholder="Enter password"
                      className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                    />
                  </div>
                </div>
              </div>
            )}

            {/* SSL Configuration */}
            {isSSLRequired && (
              <div className="space-y-4 border border-[#314158] rounded-lg p-4">
                <h3 className="font-['Fira_Code:Retina',_sans-serif] text-[#ffb86a] text-sm">SSL Configuration</h3>
                
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                      Keystore Path
                    </Label>
                    <Input
                      value={keystorePath}
                      onChange={(e) => setKeystorePath(e.target.value)}
                      placeholder="/path/to/client.keystore.jks"
                      className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                      Keystore Password
                    </Label>
                    <Input
                      type="password"
                      value={keystorePassword}
                      onChange={(e) => setKeystorePassword(e.target.value)}
                      placeholder="keystore password"
                      className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                    />
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                      Truststore Path
                    </Label>
                    <Input
                      value={truststorePath}
                      onChange={(e) => setTruststorePath(e.target.value)}
                      placeholder="/path/to/client.truststore.jks"
                      className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label className="font-['Fira_Code:Retina',_sans-serif] text-sm text-[#90a1b9]">
                      Truststore Password
                    </Label>
                    <Input
                      type="password"
                      value={truststorePassword}
                      onChange={(e) => setTruststorePassword(e.target.value)}
                      placeholder="truststore password"
                      className="bg-[#0f172b] border-[#314158] text-slate-50 font-['Fira_Code:Retina',_sans-serif] placeholder:text-[#62748E]"
                    />
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Action Buttons */}
        <div className="flex gap-3 pt-4 border-t border-[#314158] flex-shrink-0">
          {onBack && (
            <Button
              onClick={handleBack}
              variant="outline"
              className="bg-transparent border-[#314158] text-[#90a1b9] hover:bg-[#314158] hover:text-slate-50 font-['Fira_Code:Retina',_sans-serif]"
            >
              Back
            </Button>
          )}
          <Button
            onClick={handleTestConnection}
            variant="outline"
            disabled={isTesting || isConnecting}
            className="flex-1 bg-transparent border-[#314158] text-[#90a1b9] hover:bg-[#314158] hover:text-slate-50 font-['Fira_Code:Retina',_sans-serif] disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isTesting && <Loader2 className="size-4 animate-spin" />}
            {isTesting ? 'Testing…' : 'Test'}
          </Button>
          <Button
            onClick={handleSave}
            variant="outline"
            disabled={isConnecting}
            className="flex-1 bg-transparent border-[#314158] text-[#90a1b9] hover:bg-[#314158] hover:text-slate-50 font-['Fira_Code:Retina',_sans-serif] disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Save
          </Button>
          <Button
            onClick={handleConnect}
            disabled={isConnecting || isTesting}
            className="flex-1 bg-[#ffb86a] text-[#0f172b] hover:bg-[#e5a860] font-['Fira_Code:Retina',_sans-serif] disabled:opacity-70 disabled:cursor-not-allowed"
          >
            {isConnecting && <Loader2 className="size-4 animate-spin" />}
            {isConnecting ? 'Connecting…' : 'Connect'}
          </Button>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}