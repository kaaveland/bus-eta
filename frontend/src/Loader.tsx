import {useEffect, useState} from 'react'
import {type Bootstrap, loadBootstrap} from "./api.ts";
import App from "./App.tsx";

interface LoadingIssue {
  problem: string
}

function Loader() {
  const [bootstrap, setBootstrap] = useState<Bootstrap | null | LoadingIssue>(null);

  useEffect(() => {
    loadBootstrap()
      .then(setBootstrap)
      .catch((err) => setBootstrap({problem: `Failed to load: ${err}`}));
  }, []);

   if (!bootstrap) {
    return <p>Loading...</p>;
  } else if ('problem' in bootstrap) {
    return <div>
      <h1>Ouch, couldn't load the data 😭</h1>
      <p>You can check the <a href="https://kaveland.status.phare.io">status page</a>, maybe the server is down?</p>
      <p>This error message may tell you something useful if you're filing a ticket at the <a href="https://github.com/kaaveland/bus-eta">bug tracker.</a></p>
      <code>{bootstrap.problem}</code>
    </div>;
  } else {
    return (
        <App bootstrap={bootstrap}/>
    )
  }
}

export default Loader
