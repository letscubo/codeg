import { loadGatewayInternals } from './compat.mjs'
import { registerGatewayMethods } from './native-gateway.mjs'

export default {
  id: 'codeg-native-steering',
  name: 'Codeg native steering',
  register(api) {
    registerGatewayMethods(api, () => loadGatewayInternals())
  },
}
