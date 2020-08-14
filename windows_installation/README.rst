Windows Server Installation
===========================

Install Python & Java
---------------------

Python 3.8.1 and Java Runtime >=8 should be installed on the Windows
Sever.

Make sure you are using 64-bit versions of Python and Java.

Chocolatey
~~~~~~~~~~

These can be installed via the Chocolatey package manager:
https://chocolatey.org/install

.. code:: powershell

   choco install python --version 3.8.1
   choco install javaruntime

(alternative) Java Installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See official documentation:
https://docs.oracle.com/javase/10/install/installation-jdk-and-jre-microsoft-windows-platforms.htm

(alternative) Python Installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See documentation for further help to install Python:
https://docs.python.org/3/using/windows.html

.. code:: powershell

   Invoke-WebRequest -Uri "https://www.python.org/ftp/python/3.8.1/python-3.8.1-amd64.exe" -OutFile "C:/temp/python-3.8.1-installer.exe"

   "C:/temp/python-3.8.1-installer.exe" /quiet InstallAllUsers=0 PrependPath=1 Include_test=0

   py.exe -m pip install --upgrade pip  # Install latest pip version

Local Windows Server 2019 (in Virtualbox VM) for development
------------------------------------------------------------

You can configure VirtualBox to expose the remote access port on your
localhost.

Chose “NAT” Network Adapter and configure the port on “Advanced > Port
Forwarding”:

==== ======== ========= ========= ============= ==========
Name Protocol Host IP   Host Port Guest IP      Guest Port
==== ======== ========= ========= ============= ==========
SSH  TCP      127.0.0.1 2222      10.0.2.15 (*) 22
==== ======== ========= ========= ============= ==========

(*) Make sure you use your correct local “Guest IP”, take a look at
``ipconfig`` in the Windows VM.

Install forecasting_platform during development
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Ensure complete setup of ``forecasting_platform`` on a development
   system (Linux/MacOS)
2. Run ``create_zip.sh``
3. Copy ``ow-forecasting-install.zip`` and
   ``install_forecasting_platform.ps1`` to the Windows Server
4. Run ``install_forecasting_platform.ps1`` on the Windows Server (in
   VM)
5. Check that H2O server is accessible from this server, or that Java
   Runtime is installed to run the built-in H2O server
6. Open PowerShell
7. Run ``ow-forecasting-install/run_forecasting_platform.ps1 --help``

SSH for Local VM Configuration (Windows Server 2019)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create a new, non-Administrator, Windows account in your local VM
   called ``jenkins``. Remember the new password of the ``jenkins`` user
   for SSH usage.

2. Install “OpenSSH Server” via Windows “Add optional features” in the
   system settings

3. Enable OpenSSH Server service to allow remote access to your Windows
   VM:

   .. code:: powershell

      Set-Service -Name sshd -StartupType "Automatic"
      Start-Service sshd

4. You should now be able to access your local VM via
   ``ssh -p 2222 jenkins@localhost``

5. Install the “Publish Over SSH” plugin in your local Jenkins

6. Configure a new SSH Server in the “Publish over SSH” section here:
   http://localhost:8080/configure

   ::

      Name: LOCAL
      Hostname: host.docker.internal
      Username: jenkins
      (Advanced) Port: 2222
      (Advanced) Passphrase / Password: <your jenkins account password>

7. Click “Test Configuration” (should show ``Success`` left of the
   button ) and “Save”

OWEX Windows Server (Windows Server 2016) for testing
-----------------------------------------------------

Manual configuration needed:

1. Make sure the service user (PAP_service_fsc_tst) exists and can log
   in to ``owgaweuw1pap02``

2. Make sure Python and Java are installed and available for for this
   user, see `Install Python & Java <#install-python--java>`__

3. Setup SSH as described below

4. Setup PSRemoting (PowerShell Remoting)

SSH for PAP02 OWEX Windows Server (Windows Server 2016)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  accessible via Remote Desktop as ``owgaweuw1pap02``

Install OpenSSH Service (sshd)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. `Download <https://github.com/PowerShell/Win32-OpenSSH/releases>`__
   an OpenSSH release e.g:

   .. code:: powershell

      Invoke-WebRequest "https://github.com/PowerShell/Win32-OpenSSH/releases/download/v0.0.22.0/OpenSSH-Win64.zip" -OutFile "C:/temp/openssh.zip"

2. Extract the files from the zip file:

   .. code:: powershell

      Expand-Archive "C:/temp/openssh.zip" "C:/Program Files/"

3. Update the Enviroment Path:

   .. code:: powershell

      ($env:path).split(";")
      $oldpath = (Get-ItemProperty -Path "Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment" -Name PATH).path
      $newpath = "$oldpath;C:\Program Files\OpenSSH-Win64\"
      Set-ItemProperty -Path "Registry::HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Session Manager\Environment" -Name PATH -Value $newPath

4. Restart PowerShell for changes to take effect.

5. Set the current directory to OpenSSL and execute the
   ``.\install-sshd.ps1`` script:

   .. code:: powershell

      cd "C:\Program Files\OpenSSH-Win64\"
      .\install-sshd.ps1

6. Start the SSHD service:

   .. code:: powershell

      Start-Service -Name sshd

7. Set the service to start on boot:

   .. code:: powershell

      Set-Service -Name "sshd" -StartupType automatic
      Get-WMIObject win32_service -Filter "name = 'sshd'"
      Get-WMIObject win32_service | Format-Table Name, StartMode -auto

8. Open up the Firewall port 22 for SSH (ensure that the server is not
   reachable from the internet):

   .. code:: powershell

      netsh advfirewall firewall add rule name="Open Port 22" dir=in action=allow protocol=TCP localport=22

9. You should now be able to login with service account locally:

   .. code:: powershell

      ssh PAP_service_fsc_tst@127.0.0.1

   After the successful login, ``C:\Users\PAP_service_fsc_tst\`` is
   created automatically.

Configure ssh key authentication on Server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Configure in ``C:\ProgramData\ssh\sshd_config``:

   -  Enable public key authentication ``PubkeyAuthentication yes``

   -  Disable password authentication ``PasswordAuthentication no``

   -  Comment out this administrator setting::

      # Match Group administrators
      #       AuthorizedKeysFile __PROGRAMDATA__/ssh/administrators_authorized_keys

2. Generate SSH host-key with default options:

   .. code:: powershell

      cd "C:\Program Files\OpenSSL-Win64>"
      .\ssh-keygen.exe

3. Move or rename public key ``id_rsa.pub`` to ``C:\Users\PAP_service_fsc_tst\.ssh\authorized_keys``

4. Set following permissions for ``C:\Users\PAP_service_fsc_tst\.ssh\``
   and for ``authorized_keys`` files (via GUI)

   -  Properties -> Security -> Advanced -> Disable inheritance ->
      Convert inherited permissions into explicit permissions on this
      object.
   -  Then delete all users(groups) except:

      -  System
      -  Administrators
      -  current user

   -  For more information see
      https://github.com/PowerShell/Win32-OpenSSH/issues/1306#issuecomment-589995528

5. Keep private key ``id_rsa`` securely, you will need it to configure
   access from Jenkins

6. Restart the OpenSSH service:

   .. code:: powershell

      Restart-Service -Name sshd

Configure ssh private key credential in Jenkins
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

-  Jenkins is accessible via Remote Desktop from ``owgusbp1wb03``
-  In Chrome navigate to http://owgusbp1dev05.owex.corp:8080/, use your
   workbench credentials for login

1. In Jenkins store a private key ``id_rsa`` as a *Secret File*
   credential under http://owgusbp1dev02.owex.corp:8081/credentials, see
   \_SSH private key for PAP_service_fsc_tst@OWGAWEUW1PAP02_. Note that
   using the *SSH username and private key* credential option results in
   errors in the job execution

2. In a job configuration
   (e.g. http://owgusbp1dev02.owex.corp:8081/job/pap007_testing_pipeline/configure)
   you can bind the secret file path to environment variable e.g
   ``PAP02_SSH_KEY`` in *Build Environment* -> *Use secret text or file*
   -> *Bindings* section

3. This variable can be used to establish ssh connection in *Build* ->
   *Execute shell* step for example as:

   .. code:: bash

     ssh -i "$PAP02_SSH_KEY" PAP_service_fsc_tst@OWGAWEUW1PAP02 whoami

PSRemoting (PowerShell Remoting) for PAP02 OWEX Windows Server (Windows Server 2016)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  Accessible via Remote Desktop as ``owgaweuw1pap02``

1. Make sure PSRemoting is enabled

   .. code:: powershell

      Enable-PSRemoting
      Set-Service WinRM -StartMode Automatic

2. Configure a PowerShell session configuration so it works
   with `the Windows authentication "double hop" problem <https://docs.microsoft.com/en-us/powershell/scripting/learn/remoting/ps-remoting-second-hop>`__:

   .. code:: powershell

      Register-PSSessionConfiguration -Name PAP_service_fsc_tst -RunAsCredential "" -Force
      # Input the credentials for PAP_service_fsc_tst to the Windows dialog

      Set-PSSessionConfiguration -ShowSecurityDescriptorUI -Name PAP_service_fsc_tst
      # Add full control permissions for PAP_service_fsc_tst in the opened Windows dialog
