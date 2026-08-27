import { useState, useEffect } from 'react';
import { Button } from '../../ui/button';
import { ArrowLeft, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { Dialog, DialogHeader, DialogTitle, DialogDescription } from '../../ui/dialog';
import { DialogContentNoClose } from '../DialogContentNoClose';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '../../ui/select';
import { Input } from '../../ui/input';
import { Label } from '../../ui/label';
import { KafkaCluster, Topic } from '../types';
import { invoke } from '@tauri-apps/api/core';
import { open as openDialog } from '@tauri-apps/plugin-dialog';

/** Запасной список на случай, если бэкенд не ответил. GSSAPI сюда не входит:
 *  он есть не в каждой сборке (Cyrus SASL линкуется только фичей `gssapi`). */
const FALLBACK_MECHANISMS = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'OAUTHBEARER'];

interface ClusterConfigModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  cluster?: KafkaCluster | null;
  mode: 'create' | 'edit';
  onBack?: () => void;
  onSave?: (cluster: Partial<KafkaCluster>) => void;
  onConnected?: (topics: Topic[]) => void;
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
  const [sslCaBundlePath, setSslCaBundlePath] = useState<string>('');
  const [saslMechanism, setSaslMechanism] = useState<string>('PLAIN');

  const [isConnecting, setIsConnecting] = useState<boolean>(false);
  const [isTesting, setIsTesting] = useState<boolean>(false);
  const [mechanisms, setMechanisms] = useState<string[]>(FALLBACK_MECHANISMS);

  // Какие механизмы доступны, знает только бэкенд: GSSAPI требует Cyrus SASL,
  // который линкуется не во всех сборках. Спрашиваем один раз за жизнь модалки.
  useEffect(() => {
    invoke<string[]>('sasl_mechanisms')
      .then(setMechanisms)
      .catch((e) => console.error('Failed to load SASL mechanisms', e));
  }, []);

  // Load cluster data when editing
  useEffect(() => {
    if (cluster && mode === 'edit') {
      setName(cluster.name);
      setBrokers(cluster.brokers);
      setSecurityProtocol(cluster.securityProtocol);
      setUsername(cluster.username || '');
      setPassword(cluster.password || '');
      setSslCaBundlePath(cluster.sslCaBundlePath || '');
      setSaslMechanism(cluster.saslMechanism || 'PLAIN');
    } else {
      // Reset form for create mode
      setName('');
      setBrokers('localhost:9092');
      setSecurityProtocol('PLAINTEXT');
      setUsername('');
      setPassword('');
      setSslCaBundlePath('');
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
      ssl_ca_bundle_path: isSSLRequired ? sslCaBundlePath : undefined,
    };

    try {
      setIsConnecting(true);
      await invoke('cluster_connect', { payload: clusterData });
      // After successful connect, fetch topics from backend
      try {
        const topics = await invoke<Topic[]>('get_topics');
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
      toast.error('Failed to connect to Kafka cluster: ' + (e as Error).message);
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
      ssl_ca_bundle_path: isSSLRequired ? sslCaBundlePath : undefined,
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
      sslCaBundlePath: isSSLRequired ? sslCaBundlePath : undefined,
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
      <DialogContentNoClose className="max-w-2xl max-h-[90vh] bg-surface border-edge text-slate-50 flex flex-col">
        <DialogHeader className="border-b border-edge pb-4 flex-shrink-0">
          <div className="flex items-center gap-3">
            {onBack && (
              <Button
                onClick={handleBack}
                variant="ghost"
                size="sm"
                className="text-soft hover:text-brand hover:bg-edge p-2"
              >
                <ArrowLeft className="size-4" />
              </Button>
            )}
            <div>
              <DialogTitle className="font-mono text-soft text-lg">
                {mode === 'create' ? 'Create New Cluster' : 'Edit Cluster'}
              </DialogTitle>
              <DialogDescription className="text-sm text-soft">
                Configure your Kafka cluster connection settings
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>
        
        <div className="flex-1 overflow-auto">
          <div className="space-y-6 p-1 pr-3">
            {/* Cluster Name */}
            <div className="space-y-2">
              <Label className="font-mono text-sm text-soft">
                Cluster Name
              </Label>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="My Kafka Cluster"
                className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
              />
            </div>

            {/* Brokers */}
            <div className="space-y-2">
              <Label className="font-mono text-sm text-soft">
                Bootstrap Servers
              </Label>
              <Input
                value={brokers}
                onChange={(e) => setBrokers(e.target.value)}
                placeholder="localhost:9092"
                className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
              />
            </div>

            {/* Security Protocol */}
            <div className="space-y-2">
              <Label className="font-mono text-sm text-soft">
                Security Protocol
              </Label>
              <Select value={securityProtocol} onValueChange={setSecurityProtocol}>
                <SelectTrigger className="bg-surface border-edge text-slate-50 font-mono">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-surface border-edge">
                  <SelectItem value="PLAINTEXT" className="text-slate-50 font-mono focus:bg-edge">PLAINTEXT</SelectItem>
                  <SelectItem value="SSL" className="text-slate-50 font-mono focus:bg-edge">SSL</SelectItem>
                  <SelectItem value="SASL_PLAINTEXT" className="text-slate-50 font-mono focus:bg-edge">SASL_PLAINTEXT</SelectItem>
                  <SelectItem value="SASL_SSL" className="text-slate-50 font-mono focus:bg-edge">SASL_SSL</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* SASL Configuration */}
            {isSASLRequired && (
              <div className="space-y-4 border border-edge rounded-lg p-4">
                <h3 className="font-mono text-brand text-sm">SASL Configuration</h3>
                
                <div className="space-y-2">
                  <Label className="font-mono text-sm text-soft">
                    SASL Mechanism
                  </Label>
                  <Select value={saslMechanism} onValueChange={setSaslMechanism}>
                    <SelectTrigger className="bg-surface border-edge text-slate-50 font-mono">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-surface border-edge">
                      {mechanisms.map((mech) => (
                        <SelectItem key={mech} value={mech} className="text-slate-50 font-mono focus:bg-edge">{mech}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label className="font-mono text-sm text-soft">
                      Username
                    </Label>
                    <Input
                      value={username}
                      onChange={(e) => setUsername(e.target.value)}
                      placeholder="Enter username"
                      className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label className="font-mono text-sm text-soft">
                      Password
                    </Label>
                    <Input
                      type="password"
                      value={password}
                      onChange={(e) => setPassword(e.target.value)}
                      placeholder="Enter password"
                      className="bg-surface border-edge text-slate-50 font-mono placeholder:text-dim"
                    />
                  </div>
                </div>
              </div>
            )}

            {/* SSL Configuration */}
            {isSSLRequired && (
              <div className="space-y-4 border border-edge rounded-lg p-4">
                <h3 className="font-mono text-brand text-sm">SSL Configuration</h3>

                <div className="grid grid-cols-1 gap-1">
                  <div className="space-y-1">
                    <Label className="font-mono text-sm text-soft">
                      CA bundle
                    </Label>
                    <div className="flex items-center gap-3">
                      <Button
                        type="button"
                        variant="outline"
                        className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
                        onClick={async () => {
                          const selected = await openDialog({
                            title: 'Select CA bundle file',
                            multiple: false,
                            filters: [
                              { name: 'Certificates', extensions: ['crt', 'pem', 'cer'] },
                              { name: 'All Files', extensions: ['*'] },
                            ],
                          });
                          if (typeof selected === 'string') {
                            setSslCaBundlePath(selected);
                          }
                        }}
                      >
                        {sslCaBundlePath ? 'Choose another file' : 'Choose file'}
                      </Button>
                      {sslCaBundlePath && (
                        <div className="text-xs text-soft truncate max-w-[260px]" title={sslCaBundlePath}>
                          Selected: <span className="text-slate-50">{(sslCaBundlePath.split('\\').pop() || '').split('/').pop()}</span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Action Buttons */}
        <div className="flex gap-3 pt-4 border-t border-edge flex-shrink-0">
          {onBack && (
            <Button
              onClick={handleBack}
              variant="outline"
              className="bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono"
            >
              Back
            </Button>
          )}
          <Button
            onClick={handleTestConnection}
            variant="outline"
            disabled={isTesting || isConnecting}
            className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isTesting && <Loader2 className="size-4 animate-spin" />}
            {isTesting ? 'Testing…' : 'Test'}
          </Button>
          <Button
            onClick={handleSave}
            variant="outline"
            disabled={isConnecting}
            className="flex-1 bg-transparent border-edge text-soft hover:bg-edge hover:text-slate-50 font-mono disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Save
          </Button>
          <Button
            onClick={handleConnect}
            disabled={isConnecting || isTesting}
            className="flex-1 bg-brand text-surface hover:bg-brand-hover font-mono disabled:opacity-70 disabled:cursor-not-allowed"
          >
            {isConnecting && <Loader2 className="size-4 animate-spin" />}
            {isConnecting ? 'Connecting…' : 'Connect'}
          </Button>
        </div>
      </DialogContentNoClose>
    </Dialog>
  );
}